const std = @import("std");
const Message = @import("../message.zig").Message;
const WombatAdapter = @import("wombat_adapter.zig").WombatAdapter;
const KVPair = @import("wombat_adapter.zig").KVPair;

pub const MessageStorage = struct {
    adapter: *WombatAdapter,
    allocator: std.mem.Allocator,
    stats: StorageStats,

    const StorageStats = struct {
        messages_stored: u64,
        messages_retrieved: u64,
        messages_deleted: u64,
        bytes_stored: u64,
        bytes_retrieved: u64,
        storage_errors: u64,
    };

    pub fn init(allocator: std.mem.Allocator, adapter: *WombatAdapter) MessageStorage {
        return MessageStorage{
            .adapter = adapter,
            .allocator = allocator,
            .stats = StorageStats{
                .messages_stored = 0,
                .messages_retrieved = 0,
                .messages_deleted = 0,
                .bytes_stored = 0,
                .bytes_retrieved = 0,
                .storage_errors = 0,
            },
        };
    }

    pub fn storeMessage(self: *MessageStorage, queue_name: []const u8, message: *const Message) !void {
        const storage_key = try self.adapter.generateMessageKey(self.allocator, queue_name, message.id);
        defer self.allocator.free(storage_key);

        const encoded_message = try message.encodeForStorage(self.allocator);
        defer self.allocator.free(encoded_message);

        self.adapter.set(storage_key, encoded_message) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.messages_stored += 1;
        self.stats.bytes_stored += encoded_message.len;

        std.log.debug("Message stored: queue={s}, id={}, size={} bytes", .{ queue_name, message.id, encoded_message.len });
    }

    pub fn loadMessage(self: *MessageStorage, queue_name: []const u8, message_id: u64) !?Message {
        const storage_key = try self.adapter.generateMessageKey(self.allocator, queue_name, message_id);
        defer self.allocator.free(storage_key);

        const encoded_data = self.adapter.get(storage_key) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        if (encoded_data) |data| {
            defer self.allocator.free(data);

            const message = Message.decodeFromStorage(data, self.allocator) catch |err| {
                self.stats.storage_errors += 1;
                return err;
            };

            self.stats.messages_retrieved += 1;
            self.stats.bytes_retrieved += data.len;

            std.log.debug("Message loaded: queue={s}, id={}, size={} bytes", .{ queue_name, message_id, data.len });
            return message;
        }

        return null;
    }

    pub fn deleteMessage(self: *MessageStorage, queue_name: []const u8, message_id: u64) !void {
        const storage_key = try self.adapter.generateMessageKey(self.allocator, queue_name, message_id);
        defer self.allocator.free(storage_key);

        self.adapter.delete(storage_key) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.messages_deleted += 1;
        std.log.debug("Message deleted: queue={s}, id={}", .{ queue_name, message_id });
    }

    pub fn storeMessageBatch(self: *MessageStorage, queue_name: []const u8, messages: []const Message) !void {
        var entries = std.ArrayList(KVPair).init(self.allocator);
        defer entries.deinit();
        defer {
            for (entries.items) |entry| {
                entry.deinit(self.allocator);
            }
        }

        // Prepare all key-value pairs
        for (messages) |message| {
            const storage_key = try self.adapter.generateMessageKey(self.allocator, queue_name, message.id);
            const encoded_message = try message.encodeForStorage(self.allocator);

            try entries.append(KVPair{
                .key = storage_key,
                .value = encoded_message,
            });

            self.stats.bytes_stored += encoded_message.len;
        }

        // Perform batch operation
        self.adapter.setBatch(entries.items) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.messages_stored += messages.len;
        std.log.debug("Message batch stored: queue={s}, count={}", .{ queue_name, messages.len });
    }

    pub fn loadMessageBatch(self: *MessageStorage, queue_name: []const u8, message_ids: []const u64) !std.ArrayList(Message) {
        var keys = std.ArrayList([]const u8).init(self.allocator);
        defer keys.deinit();
        defer {
            for (keys.items) |key| {
                self.allocator.free(key);
            }
        }

        // Generate storage keys
        for (message_ids) |message_id| {
            const storage_key = try self.adapter.generateMessageKey(self.allocator, queue_name, message_id);
            try keys.append(storage_key);
        }

        // Perform batch get
        var encoded_values = self.adapter.getBatch(keys.items) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };
        defer encoded_values.deinit();
        defer {
            for (encoded_values.items) |value| {
                if (value) |v| {
                    self.allocator.free(v);
                }
            }
        }

        var messages = std.ArrayList(Message).init(self.allocator);
        errdefer messages.deinit();

        // Decode messages
        for (encoded_values.items, 0..) |maybe_value, i| {
            if (maybe_value) |value| {
                const message = Message.decodeFromStorage(value, self.allocator) catch |err| {
                    self.stats.storage_errors += 1;
                    // Clean up already decoded messages
                    for (messages.items) |*msg| {
                        msg.deinit();
                    }
                    return err;
                };

                try messages.append(message);
                self.stats.bytes_retrieved += value.len;
            } else {
                std.log.warn("Message not found: queue={s}, id={}", .{ queue_name, message_ids[i] });
            }
        }

        self.stats.messages_retrieved += messages.items.len;
        std.log.debug("Message batch loaded: queue={s}, found={}/{}", .{ queue_name, messages.items.len, message_ids.len });

        return messages;
    }

    pub fn deleteMessageBatch(self: *MessageStorage, queue_name: []const u8, message_ids: []const u64) !void {
        var keys = std.ArrayList([]const u8).init(self.allocator);
        defer keys.deinit();
        defer {
            for (keys.items) |key| {
                self.allocator.free(key);
            }
        }

        // Generate storage keys
        for (message_ids) |message_id| {
            const storage_key = try self.adapter.generateMessageKey(self.allocator, queue_name, message_id);
            try keys.append(storage_key);
        }

        // Perform batch delete
        self.adapter.deleteBatch(keys.items) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.messages_deleted += message_ids.len;
        std.log.debug("Message batch deleted: queue={s}, count={}", .{ queue_name, message_ids.len });
    }

    pub fn getQueueMessages(self: *MessageStorage, queue_name: []const u8) !std.ArrayList(Message) {
        const prefix = try std.fmt.allocPrint(self.allocator, "queue:{s}:msg:", .{queue_name});
        defer self.allocator.free(prefix);

        const entries = try self.adapter.scan(prefix, self.allocator);
        defer entries.deinit();
        defer {
            for (entries.items) |entry| {
                entry.deinit(self.allocator);
            }
        }

        var messages = std.ArrayList(Message).init(self.allocator);
        errdefer messages.deinit();

        for (entries.items) |entry| {
            const message = Message.decodeFromStorage(entry.value, self.allocator) catch |err| {
                self.stats.storage_errors += 1;
                std.log.err("Failed to decode message for queue {s}: {}", .{ queue_name, err });
                continue;
            };

            try messages.append(message);
            self.stats.bytes_retrieved += entry.value.len;
        }

        self.stats.messages_retrieved += messages.items.len;
        std.log.debug("Queue messages loaded: queue={s}, count={}", .{ queue_name, messages.items.len });

        return messages;
    }

    pub fn deleteQueueMessages(self: *MessageStorage, queue_name: []const u8) !u32 {
        const prefix = try std.fmt.allocPrint(self.allocator, "queue:{s}:msg:", .{queue_name});
        defer self.allocator.free(prefix);

        const entries = try self.adapter.scan(prefix, self.allocator);
        defer entries.deinit();
        defer {
            for (entries.items) |entry| {
                entry.deinit(self.allocator);
            }
        }

        var keys = std.ArrayList([]const u8).init(self.allocator);
        defer keys.deinit();

        for (entries.items) |entry| {
            try keys.append(try self.allocator.dupe(u8, entry.key));
        }
        defer {
            for (keys.items) |key| {
                self.allocator.free(key);
            }
        }

        if (keys.items.len > 0) {
            self.adapter.deleteBatch(keys.items) catch |err| {
                self.stats.storage_errors += 1;
                return err;
            };
        }

        const deleted_count = @as(u32, @intCast(keys.items.len));
        self.stats.messages_deleted += deleted_count;
        std.log.debug("Queue messages deleted: queue={s}, count={}", .{ queue_name, deleted_count });

        return deleted_count;
    }

    pub fn getMessageCount(self: *MessageStorage, queue_name: []const u8) !u32 {
        const prefix = try std.fmt.allocPrint(self.allocator, "queue:{s}:msg:", .{queue_name});
        defer self.allocator.free(prefix);

        const entries = try self.adapter.scan(prefix, self.allocator);
        defer entries.deinit();
        defer {
            for (entries.items) |entry| {
                entry.deinit(self.allocator);
            }
        }

        return @intCast(entries.items.len);
    }

    pub fn getStats(self: *const MessageStorage) StorageStats {
        return self.stats;
    }

    pub fn resetStats(self: *MessageStorage) void {
        self.stats = StorageStats{
            .messages_stored = 0,
            .messages_retrieved = 0,
            .messages_deleted = 0,
            .bytes_stored = 0,
            .bytes_retrieved = 0,
            .storage_errors = 0,
        };
    }

    pub fn getStatsJson(self: *const MessageStorage, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("messages_stored", std.json.Value{ .integer = @intCast(self.stats.messages_stored) });
        try stats.put("messages_retrieved", std.json.Value{ .integer = @intCast(self.stats.messages_retrieved) });
        try stats.put("messages_deleted", std.json.Value{ .integer = @intCast(self.stats.messages_deleted) });
        try stats.put("bytes_stored", std.json.Value{ .integer = @intCast(self.stats.bytes_stored) });
        try stats.put("bytes_retrieved", std.json.Value{ .integer = @intCast(self.stats.bytes_retrieved) });
        try stats.put("storage_errors", std.json.Value{ .integer = @intCast(self.stats.storage_errors) });

        return std.json.Value{ .object = stats };
    }

    // Maintenance operations
    pub fn compact(self: *MessageStorage) !void {
        try self.adapter.runCompaction();
        std.log.info("Message storage compaction completed");
    }

    pub fn backup(self: *MessageStorage, backup_dir: []const u8) !void {
        try self.adapter.backup(backup_dir);
        std.log.info("Message storage backup completed to: {s}", .{backup_dir});
    }

    pub fn getStorageSize(self: *MessageStorage) u64 {
        return self.adapter.getSize();
    }

    pub fn getMemoryUsage(self: *MessageStorage) u64 {
        return self.adapter.getMemoryUsage();
    }

    pub fn healthCheck(self: *MessageStorage) !bool {
        return self.adapter.healthCheck();
    }
};

test "message storage operations" {
    const allocator = std.testing.allocator;

    // This test would need a real WombatAdapter instance
    // For now, we'll skip the actual storage tests
    std.testing.skip();
}

test "message storage batch operations" {
    const allocator = std.testing.allocator;

    // This test would need a real WombatAdapter instance
    // For now, we'll skip the actual storage tests
    std.testing.skip();
}

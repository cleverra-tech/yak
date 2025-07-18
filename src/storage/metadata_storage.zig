const std = @import("std");
const WombatAdapter = @import("wombat_adapter.zig").WombatAdapter;
const Exchange = @import("../routing/exchange.zig").Exchange;
const Queue = @import("../routing/queue.zig").Queue;
const Binding = @import("../routing/binding.zig").Binding;

pub const MetadataStorage = struct {
    adapter: *WombatAdapter,
    allocator: std.mem.Allocator,
    stats: StorageStats,

    const StorageStats = struct {
        exchanges_stored: u64,
        queues_stored: u64,
        bindings_stored: u64,
        metadata_operations: u64,
        storage_errors: u64,
    };

    pub fn init(allocator: std.mem.Allocator, adapter: *WombatAdapter) MetadataStorage {
        return MetadataStorage{
            .adapter = adapter,
            .allocator = allocator,
            .stats = StorageStats{
                .exchanges_stored = 0,
                .queues_stored = 0,
                .bindings_stored = 0,
                .metadata_operations = 0,
                .storage_errors = 0,
            },
        };
    }

    // Exchange metadata operations
    pub fn storeExchange(self: *MetadataStorage, exchange: *const Exchange) !void {
        const storage_key = try self.adapter.generateExchangeMetadataKey(self.allocator, exchange.name);
        defer self.allocator.free(storage_key);

        const metadata = ExchangeMetadata{
            .name = exchange.name,
            .exchange_type = exchange.exchange_type,
            .durable = exchange.durable,
            .auto_delete = exchange.auto_delete,
            .internal = exchange.internal,
            .arguments = exchange.arguments,
        };

        const encoded = try std.json.stringifyAlloc(self.allocator, metadata, .{});
        defer self.allocator.free(encoded);

        self.adapter.set(storage_key, encoded) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.exchanges_stored += 1;
        self.stats.metadata_operations += 1;

        std.log.debug("Exchange metadata stored: {s}", .{exchange.name});
    }

    pub fn loadExchange(self: *MetadataStorage, exchange_name: []const u8) !?ExchangeMetadata {
        const storage_key = try self.adapter.generateExchangeMetadataKey(self.allocator, exchange_name);
        defer self.allocator.free(storage_key);

        const encoded_data = self.adapter.get(storage_key) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        if (encoded_data) |data| {
            defer self.allocator.free(data);

            var parsed = std.json.parseFromSlice(ExchangeMetadata, self.allocator, data, .{}) catch |err| {
                self.stats.storage_errors += 1;
                return err;
            };
            defer parsed.deinit();

            self.stats.metadata_operations += 1;
            return try cloneExchangeMetadata(self.allocator, parsed.value);
        }

        return null;
    }

    pub fn deleteExchange(self: *MetadataStorage, exchange_name: []const u8) !void {
        const storage_key = try self.adapter.generateExchangeMetadataKey(self.allocator, exchange_name);
        defer self.allocator.free(storage_key);

        self.adapter.delete(storage_key) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.metadata_operations += 1;
        std.log.debug("Exchange metadata deleted: {s}", .{exchange_name});
    }

    pub fn listExchanges(self: *MetadataStorage) !std.ArrayList(ExchangeMetadata) {
        const prefix = "exchange:";
        const suffix = ":metadata";

        const entries = try self.adapter.scan(prefix, self.allocator);
        defer entries.deinit();
        defer {
            for (entries.items) |entry| {
                entry.deinit(self.allocator);
            }
        }

        var exchanges = std.ArrayList(ExchangeMetadata).init(self.allocator);
        errdefer exchanges.deinit();

        for (entries.items) |entry| {
            if (std.mem.endsWith(u8, entry.key, suffix)) {
                var parsed = std.json.parseFromSlice(ExchangeMetadata, self.allocator, entry.value, .{}) catch |err| {
                    self.stats.storage_errors += 1;
                    std.log.err("Failed to parse exchange metadata: {}", .{err});
                    continue;
                };
                defer parsed.deinit();

                const metadata = try cloneExchangeMetadata(self.allocator, parsed.value);
                try exchanges.append(metadata);
            }
        }

        self.stats.metadata_operations += 1;
        return exchanges;
    }

    // Queue metadata operations
    pub fn storeQueue(self: *MetadataStorage, queue: *const Queue) !void {
        const storage_key = try self.adapter.generateQueueMetadataKey(self.allocator, queue.name);
        defer self.allocator.free(storage_key);

        const metadata = QueueMetadata{
            .name = queue.name,
            .durable = queue.durable,
            .exclusive = queue.exclusive,
            .auto_delete = queue.auto_delete,
            .arguments = queue.arguments,
            .max_length = queue.max_length,
            .message_ttl = queue.message_ttl,
        };

        const encoded = try std.json.stringifyAlloc(self.allocator, metadata, .{});
        defer self.allocator.free(encoded);

        self.adapter.set(storage_key, encoded) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.queues_stored += 1;
        self.stats.metadata_operations += 1;

        std.log.debug("Queue metadata stored: {s}", .{queue.name});
    }

    pub fn loadQueue(self: *MetadataStorage, queue_name: []const u8) !?QueueMetadata {
        const storage_key = try self.adapter.generateQueueMetadataKey(self.allocator, queue_name);
        defer self.allocator.free(storage_key);

        const encoded_data = self.adapter.get(storage_key) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        if (encoded_data) |data| {
            defer self.allocator.free(data);

            var parsed = std.json.parseFromSlice(QueueMetadata, self.allocator, data, .{}) catch |err| {
                self.stats.storage_errors += 1;
                return err;
            };
            defer parsed.deinit();

            self.stats.metadata_operations += 1;
            return try cloneQueueMetadata(self.allocator, parsed.value);
        }

        return null;
    }

    pub fn deleteQueue(self: *MetadataStorage, queue_name: []const u8) !void {
        const storage_key = try self.adapter.generateQueueMetadataKey(self.allocator, queue_name);
        defer self.allocator.free(storage_key);

        self.adapter.delete(storage_key) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.metadata_operations += 1;
        std.log.debug("Queue metadata deleted: {s}", .{queue_name});
    }

    pub fn listQueues(self: *MetadataStorage) !std.ArrayList(QueueMetadata) {
        const prefix = "queue:";
        const suffix = ":metadata";

        const entries = try self.adapter.scan(prefix, self.allocator);
        defer entries.deinit();
        defer {
            for (entries.items) |entry| {
                entry.deinit(self.allocator);
            }
        }

        var queues = std.ArrayList(QueueMetadata).init(self.allocator);
        errdefer queues.deinit();

        for (entries.items) |entry| {
            if (std.mem.endsWith(u8, entry.key, suffix)) {
                var parsed = std.json.parseFromSlice(QueueMetadata, self.allocator, entry.value, .{}) catch |err| {
                    self.stats.storage_errors += 1;
                    std.log.err("Failed to parse queue metadata: {}", .{err});
                    continue;
                };
                defer parsed.deinit();

                const metadata = try cloneQueueMetadata(self.allocator, parsed.value);
                try queues.append(metadata);
            }
        }

        self.stats.metadata_operations += 1;
        return queues;
    }

    // Binding operations
    pub fn storeBinding(self: *MetadataStorage, binding: *const Binding) !void {
        const storage_key = try self.adapter.generateBindingKey(
            self.allocator,
            binding.exchange_name,
            binding.queue_name,
            binding.routing_key,
        );
        defer self.allocator.free(storage_key);

        const encoded = try binding.encodeForStorage(self.allocator);
        defer self.allocator.free(encoded);

        self.adapter.set(storage_key, encoded) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.bindings_stored += 1;
        self.stats.metadata_operations += 1;

        std.log.debug("Binding stored: {s} -> {s} (key: {s})", .{ binding.exchange_name, binding.queue_name, binding.routing_key });
    }

    pub fn deleteBinding(self: *MetadataStorage, exchange_name: []const u8, queue_name: []const u8, routing_key: []const u8) !void {
        const storage_key = try self.adapter.generateBindingKey(self.allocator, exchange_name, queue_name, routing_key);
        defer self.allocator.free(storage_key);

        self.adapter.delete(storage_key) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.metadata_operations += 1;
        std.log.debug("Binding deleted: {s} -> {s} (key: {s})", .{ exchange_name, queue_name, routing_key });
    }

    pub fn listBindings(self: *MetadataStorage) !std.ArrayList(Binding) {
        const prefix = "binding:";

        const entries = try self.adapter.scan(prefix, self.allocator);
        defer entries.deinit();
        defer {
            for (entries.items) |entry| {
                entry.deinit(self.allocator);
            }
        }

        var bindings = std.ArrayList(Binding).init(self.allocator);
        errdefer bindings.deinit();

        for (entries.items) |entry| {
            const binding = Binding.decodeFromStorage(entry.value, self.allocator) catch |err| {
                self.stats.storage_errors += 1;
                std.log.err("Failed to decode binding: {}", .{err});
                continue;
            };

            try bindings.append(binding);
        }

        self.stats.metadata_operations += 1;
        return bindings;
    }

    pub fn listBindingsForExchange(self: *MetadataStorage, exchange_name: []const u8) !std.ArrayList(Binding) {
        const prefix = try std.fmt.allocPrint(self.allocator, "binding:{s}:", .{exchange_name});
        defer self.allocator.free(prefix);

        const entries = try self.adapter.scan(prefix, self.allocator);
        defer entries.deinit();
        defer {
            for (entries.items) |entry| {
                entry.deinit(self.allocator);
            }
        }

        var bindings = std.ArrayList(Binding).init(self.allocator);
        errdefer bindings.deinit();

        for (entries.items) |entry| {
            const binding = Binding.decodeFromStorage(entry.value, self.allocator) catch |err| {
                self.stats.storage_errors += 1;
                std.log.err("Failed to decode binding: {}", .{err});
                continue;
            };

            try bindings.append(binding);
        }

        self.stats.metadata_operations += 1;
        return bindings;
    }

    // Server configuration operations
    pub fn storeServerConfig(self: *MetadataStorage, config: anytype) !void {
        const storage_key = try self.adapter.generateServerConfigKey(self.allocator);
        defer self.allocator.free(storage_key);

        const encoded = try std.json.stringifyAlloc(self.allocator, config, .{});
        defer self.allocator.free(encoded);

        self.adapter.set(storage_key, encoded) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        self.stats.metadata_operations += 1;
        std.log.debug("Server configuration stored");
    }

    pub fn loadServerConfig(self: *MetadataStorage, comptime T: type) !?T {
        const storage_key = try self.adapter.generateServerConfigKey(self.allocator);
        defer self.allocator.free(storage_key);

        const encoded_data = self.adapter.get(storage_key) catch |err| {
            self.stats.storage_errors += 1;
            return err;
        };

        if (encoded_data) |data| {
            defer self.allocator.free(data);

            var parsed = std.json.parseFromSlice(T, self.allocator, data, .{}) catch |err| {
                self.stats.storage_errors += 1;
                return err;
            };
            defer parsed.deinit();

            self.stats.metadata_operations += 1;
            return parsed.value;
        }

        return null;
    }

    pub fn getStats(self: *const MetadataStorage) StorageStats {
        return self.stats;
    }

    pub fn getStatsJson(self: *const MetadataStorage, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("exchanges_stored", std.json.Value{ .integer = @intCast(self.stats.exchanges_stored) });
        try stats.put("queues_stored", std.json.Value{ .integer = @intCast(self.stats.queues_stored) });
        try stats.put("bindings_stored", std.json.Value{ .integer = @intCast(self.stats.bindings_stored) });
        try stats.put("metadata_operations", std.json.Value{ .integer = @intCast(self.stats.metadata_operations) });
        try stats.put("storage_errors", std.json.Value{ .integer = @intCast(self.stats.storage_errors) });

        return std.json.Value{ .object = stats };
    }
};

// Metadata structures for JSON serialization
const ExchangeMetadata = struct {
    name: []const u8,
    exchange_type: Exchange.ExchangeType,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    arguments: ?[]const u8,

    pub fn deinit(self: *const ExchangeMetadata, allocator: std.mem.Allocator) void {
        if (self.arguments) |args| {
            allocator.free(args);
        }
    }
};

const QueueMetadata = struct {
    name: []const u8,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    arguments: ?[]const u8,
    max_length: ?u32,
    message_ttl: ?u32,

    pub fn deinit(self: *const QueueMetadata, allocator: std.mem.Allocator) void {
        if (self.arguments) |args| {
            allocator.free(args);
        }
    }
};

// Helper functions for cloning metadata
fn cloneExchangeMetadata(allocator: std.mem.Allocator, metadata: ExchangeMetadata) !ExchangeMetadata {
    return ExchangeMetadata{
        .name = try allocator.dupe(u8, metadata.name),
        .exchange_type = metadata.exchange_type,
        .durable = metadata.durable,
        .auto_delete = metadata.auto_delete,
        .internal = metadata.internal,
        .arguments = if (metadata.arguments) |args| try allocator.dupe(u8, args) else null,
    };
}

fn cloneQueueMetadata(allocator: std.mem.Allocator, metadata: QueueMetadata) !QueueMetadata {
    return QueueMetadata{
        .name = try allocator.dupe(u8, metadata.name),
        .durable = metadata.durable,
        .exclusive = metadata.exclusive,
        .auto_delete = metadata.auto_delete,
        .arguments = if (metadata.arguments) |args| try allocator.dupe(u8, args) else null,
        .max_length = metadata.max_length,
        .message_ttl = metadata.message_ttl,
    };
}

test "metadata storage operations" {
    const allocator = std.testing.allocator;

    // This test would need a real WombatAdapter instance
    // For now, we'll skip the actual storage tests
    std.testing.skip();
}

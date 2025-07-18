const std = @import("std");

pub const QoSManager = struct {
    // Global QoS settings
    global_prefetch_count: u16,
    global_prefetch_size: u32,
    global_unacked_count: u32,
    global_unacked_size: u32,

    // Per-channel QoS tracking
    channel_qos: std.HashMap(u16, ChannelQoS, std.hash_map.AutoContext(u16), std.hash_map.default_max_load_percentage),

    // Per-consumer QoS tracking
    consumer_qos: std.HashMap([]const u8, ConsumerQoS, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),

    // Per-connection QoS tracking
    connection_qos: std.HashMap(u64, ConnectionQoS, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,

    const ChannelQoS = struct {
        prefetch_count: u16,
        prefetch_size: u32,
        unacked_count: u32,
        unacked_size: u32,
        global: bool,
    };

    const ConsumerQoS = struct {
        prefetch_count: u16,
        prefetch_size: u32,
        unacked_count: u32,
        unacked_size: u32,
        channel_id: u16,
        connection_id: u64,
    };

    const ConnectionQoS = struct {
        prefetch_count: u16,
        prefetch_size: u32,
        unacked_count: u32,
        unacked_size: u32,
        channels: std.ArrayList(u16),
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator) ConnectionQoS {
            return ConnectionQoS{
                .prefetch_count = 0,
                .prefetch_size = 0,
                .unacked_count = 0,
                .unacked_size = 0,
                .channels = std.ArrayList(u16).init(allocator),
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *ConnectionQoS) void {
            self.channels.deinit();
        }

        pub fn addChannel(self: *ConnectionQoS, channel_id: u16) !void {
            for (self.channels.items) |existing| {
                if (existing == channel_id) return; // Already exists
            }
            try self.channels.append(channel_id);
        }

        pub fn removeChannel(self: *ConnectionQoS, channel_id: u16) void {
            for (self.channels.items, 0..) |existing, i| {
                if (existing == channel_id) {
                    _ = self.channels.swapRemove(i);
                    return;
                }
            }
        }
    };

    pub fn init(allocator: std.mem.Allocator) QoSManager {
        return QoSManager{
            .global_prefetch_count = 0,
            .global_prefetch_size = 0,
            .global_unacked_count = 0,
            .global_unacked_size = 0,
            .channel_qos = std.HashMap(u16, ChannelQoS, std.hash_map.AutoContext(u16), std.hash_map.default_max_load_percentage).init(allocator),
            .consumer_qos = std.HashMap([]const u8, ConsumerQoS, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .connection_qos = std.HashMap(u64, ConnectionQoS, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *QoSManager) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Clean up connection QoS
        var conn_iterator = self.connection_qos.iterator();
        while (conn_iterator.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.connection_qos.deinit();

        // Clean up consumer QoS
        var consumer_iterator = self.consumer_qos.iterator();
        while (consumer_iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.consumer_qos.deinit();

        self.channel_qos.deinit();
    }

    pub fn canConsume(self: *QoSManager, channel_id: u16, connection_id: u64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check global limits
        if (self.global_prefetch_count > 0 and self.global_unacked_count >= self.global_prefetch_count) {
            return false;
        }

        if (self.global_prefetch_size > 0 and self.global_unacked_size >= self.global_prefetch_size) {
            return false;
        }

        // Check connection limits
        if (self.connection_qos.get(connection_id)) |conn_qos| {
            if (conn_qos.prefetch_count > 0 and conn_qos.unacked_count >= conn_qos.prefetch_count) {
                return false;
            }
            if (conn_qos.prefetch_size > 0 and conn_qos.unacked_size >= conn_qos.prefetch_size) {
                return false;
            }
        }

        // Check channel limits
        if (self.channel_qos.get(channel_id)) |channel_qos| {
            if (channel_qos.global) {
                // Global channel QoS already checked above
                return true;
            }

            if (channel_qos.prefetch_count > 0 and channel_qos.unacked_count >= channel_qos.prefetch_count) {
                return false;
            }
            if (channel_qos.prefetch_size > 0 and channel_qos.unacked_size >= channel_qos.prefetch_size) {
                return false;
            }
        }

        return true;
    }

    pub fn canConsumeConsumer(self: *QoSManager, consumer_tag: []const u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.consumer_qos.get(consumer_tag)) |consumer_qos| {
            if (consumer_qos.prefetch_count > 0 and consumer_qos.unacked_count >= consumer_qos.prefetch_count) {
                return false;
            }
            if (consumer_qos.prefetch_size > 0 and consumer_qos.unacked_size >= consumer_qos.prefetch_size) {
                return false;
            }

            // Also check channel and connection limits
            return self.canConsume(consumer_qos.channel_id, consumer_qos.connection_id);
        }

        return true;
    }

    pub fn setChannelQoS(self: *QoSManager, channel_id: u16, prefetch_count: u16, prefetch_size: u32, global: bool) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (global) {
            self.global_prefetch_count = prefetch_count;
            self.global_prefetch_size = prefetch_size;
            std.log.debug("Global QoS set: prefetch_count={}, prefetch_size={}", .{ prefetch_count, prefetch_size });
        } else {
            try self.channel_qos.put(channel_id, ChannelQoS{
                .prefetch_count = prefetch_count,
                .prefetch_size = prefetch_size,
                .unacked_count = 0,
                .unacked_size = 0,
                .global = false,
            });
            std.log.debug("Channel QoS set for channel {}: prefetch_count={}, prefetch_size={}", .{ channel_id, prefetch_count, prefetch_size });
        }
    }

    pub fn setConnectionQoS(self: *QoSManager, connection_id: u64, prefetch_count: u16, prefetch_size: u32) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const result = try self.connection_qos.getOrPut(connection_id);
        if (!result.found_existing) {
            result.value_ptr.* = ConnectionQoS.init(self.allocator);
        }

        result.value_ptr.prefetch_count = prefetch_count;
        result.value_ptr.prefetch_size = prefetch_size;

        std.log.debug("Connection QoS set for connection {}: prefetch_count={}, prefetch_size={}", .{ connection_id, prefetch_count, prefetch_size });
    }

    pub fn setConsumerQoS(
        self: *QoSManager,
        consumer_tag: []const u8,
        channel_id: u16,
        connection_id: u64,
        prefetch_count: u16,
        prefetch_size: u32,
    ) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const owned_tag = try self.allocator.dupe(u8, consumer_tag);
        try self.consumer_qos.put(owned_tag, ConsumerQoS{
            .prefetch_count = prefetch_count,
            .prefetch_size = prefetch_size,
            .unacked_count = 0,
            .unacked_size = 0,
            .channel_id = channel_id,
            .connection_id = connection_id,
        });

        std.log.debug("Consumer QoS set for {s}: prefetch_count={}, prefetch_size={}", .{ consumer_tag, prefetch_count, prefetch_size });
    }

    pub fn messageDelivered(self: *QoSManager, channel_id: u16, connection_id: u64, consumer_tag: ?[]const u8, message_size: u32) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Update global counters
        self.global_unacked_count += 1;
        self.global_unacked_size += message_size;

        // Update connection counters
        if (self.connection_qos.getPtr(connection_id)) |conn_qos| {
            conn_qos.unacked_count += 1;
            conn_qos.unacked_size += message_size;
        }

        // Update channel counters
        if (self.channel_qos.getPtr(channel_id)) |channel_qos| {
            if (!channel_qos.global) {
                channel_qos.unacked_count += 1;
                channel_qos.unacked_size += message_size;
            }
        }

        // Update consumer counters
        if (consumer_tag) |tag| {
            if (self.consumer_qos.getPtr(tag)) |consumer_qos| {
                consumer_qos.unacked_count += 1;
                consumer_qos.unacked_size += message_size;
            }
        }
    }

    pub fn messageAcknowledged(self: *QoSManager, channel_id: u16, connection_id: u64, consumer_tag: ?[]const u8, message_size: u32) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Update global counters
        self.global_unacked_count = if (self.global_unacked_count > 0) self.global_unacked_count - 1 else 0;
        self.global_unacked_size = if (self.global_unacked_size >= message_size) self.global_unacked_size - message_size else 0;

        // Update connection counters
        if (self.connection_qos.getPtr(connection_id)) |conn_qos| {
            conn_qos.unacked_count = if (conn_qos.unacked_count > 0) conn_qos.unacked_count - 1 else 0;
            conn_qos.unacked_size = if (conn_qos.unacked_size >= message_size) conn_qos.unacked_size - message_size else 0;
        }

        // Update channel counters
        if (self.channel_qos.getPtr(channel_id)) |channel_qos| {
            if (!channel_qos.global) {
                channel_qos.unacked_count = if (channel_qos.unacked_count > 0) channel_qos.unacked_count - 1 else 0;
                channel_qos.unacked_size = if (channel_qos.unacked_size >= message_size) channel_qos.unacked_size - message_size else 0;
            }
        }

        // Update consumer counters
        if (consumer_tag) |tag| {
            if (self.consumer_qos.getPtr(tag)) |consumer_qos| {
                consumer_qos.unacked_count = if (consumer_qos.unacked_count > 0) consumer_qos.unacked_count - 1 else 0;
                consumer_qos.unacked_size = if (consumer_qos.unacked_size >= message_size) consumer_qos.unacked_size - message_size else 0;
            }
        }
    }

    pub fn addConnection(self: *QoSManager, connection_id: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (!self.connection_qos.contains(connection_id)) {
            try self.connection_qos.put(connection_id, ConnectionQoS.init(self.allocator));
        }
    }

    pub fn removeConnection(self: *QoSManager, connection_id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.connection_qos.fetchRemove(connection_id)) |entry| {
            var conn_qos = entry.value;
            conn_qos.deinit();
        }

        // Remove all consumers for this connection
        var consumer_iterator = self.consumer_qos.iterator();
        var consumers_to_remove = std.ArrayList([]const u8).init(self.allocator);
        defer consumers_to_remove.deinit();

        while (consumer_iterator.next()) |entry| {
            if (entry.value_ptr.connection_id == connection_id) {
                consumers_to_remove.append(entry.key_ptr.*) catch {};
            }
        }

        for (consumers_to_remove.items) |consumer_tag| {
            self.removeConsumerQoS(consumer_tag);
        }
    }

    pub fn addChannel(self: *QoSManager, connection_id: u64, channel_id: u16) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.connection_qos.getPtr(connection_id)) |conn_qos| {
            try conn_qos.addChannel(channel_id);
        }
    }

    pub fn removeChannel(self: *QoSManager, connection_id: u64, channel_id: u16) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.connection_qos.getPtr(connection_id)) |conn_qos| {
            conn_qos.removeChannel(channel_id);
        }

        // Remove channel QoS
        _ = self.channel_qos.remove(channel_id);

        // Remove all consumers for this channel
        var consumer_iterator = self.consumer_qos.iterator();
        var consumers_to_remove = std.ArrayList([]const u8).init(self.allocator);
        defer consumers_to_remove.deinit();

        while (consumer_iterator.next()) |entry| {
            if (entry.value_ptr.channel_id == channel_id and entry.value_ptr.connection_id == connection_id) {
                consumers_to_remove.append(entry.key_ptr.*) catch {};
            }
        }

        for (consumers_to_remove.items) |consumer_tag| {
            self.removeConsumerQoS(consumer_tag);
        }
    }

    pub fn removeConsumerQoS(self: *QoSManager, consumer_tag: []const u8) void {
        if (self.consumer_qos.fetchRemove(consumer_tag)) |entry| {
            self.allocator.free(entry.key);
        }
    }

    pub fn getGlobalStats(self: *const QoSManager) QoSStats {
        return QoSStats{
            .prefetch_count = self.global_prefetch_count,
            .prefetch_size = self.global_prefetch_size,
            .unacked_count = self.global_unacked_count,
            .unacked_size = self.global_unacked_size,
        };
    }

    pub fn getChannelStats(self: *const QoSManager, channel_id: u16) ?QoSStats {
        if (self.channel_qos.get(channel_id)) |channel_qos| {
            return QoSStats{
                .prefetch_count = channel_qos.prefetch_count,
                .prefetch_size = channel_qos.prefetch_size,
                .unacked_count = channel_qos.unacked_count,
                .unacked_size = channel_qos.unacked_size,
            };
        }
        return null;
    }

    pub fn getConnectionStats(self: *const QoSManager, connection_id: u64) ?QoSStats {
        if (self.connection_qos.get(connection_id)) |conn_qos| {
            return QoSStats{
                .prefetch_count = conn_qos.prefetch_count,
                .prefetch_size = conn_qos.prefetch_size,
                .unacked_count = conn_qos.unacked_count,
                .unacked_size = conn_qos.unacked_size,
            };
        }
        return null;
    }

    pub fn getConsumerStats(self: *const QoSManager, consumer_tag: []const u8) ?QoSStats {
        if (self.consumer_qos.get(consumer_tag)) |consumer_qos| {
            return QoSStats{
                .prefetch_count = consumer_qos.prefetch_count,
                .prefetch_size = consumer_qos.prefetch_size,
                .unacked_count = consumer_qos.unacked_count,
                .unacked_size = consumer_qos.unacked_size,
            };
        }
        return null;
    }

    pub fn getAllStats(self: *const QoSManager, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        // Global stats
        var global_stats = std.json.ObjectMap.init(allocator);
        try global_stats.put("prefetch_count", std.json.Value{ .integer = @intCast(self.global_prefetch_count) });
        try global_stats.put("prefetch_size", std.json.Value{ .integer = @intCast(self.global_prefetch_size) });
        try global_stats.put("unacked_count", std.json.Value{ .integer = @intCast(self.global_unacked_count) });
        try global_stats.put("unacked_size", std.json.Value{ .integer = @intCast(self.global_unacked_size) });
        try stats.put("global", std.json.Value{ .object = global_stats });

        // Counts
        try stats.put("channels", std.json.Value{ .integer = @intCast(self.channel_qos.count()) });
        try stats.put("connections", std.json.Value{ .integer = @intCast(self.connection_qos.count()) });
        try stats.put("consumers", std.json.Value{ .integer = @intCast(self.consumer_qos.count()) });

        return std.json.Value{ .object = stats };
    }
};

pub const QoSStats = struct {
    prefetch_count: u16,
    prefetch_size: u32,
    unacked_count: u32,
    unacked_size: u32,
};

test "qos manager basic operations" {
    const allocator = std.testing.allocator;

    var qos_manager = QoSManager.init(allocator);
    defer qos_manager.deinit();

    // Initially should allow consumption
    try std.testing.expect(qos_manager.canConsume(1, 100));

    // Set channel QoS
    try qos_manager.setChannelQoS(1, 5, 1024, false);

    // Should still allow consumption
    try std.testing.expect(qos_manager.canConsume(1, 100));

    // Simulate message delivery
    qos_manager.messageDelivered(1, 100, null, 100);
    qos_manager.messageDelivered(1, 100, null, 100);
    qos_manager.messageDelivered(1, 100, null, 100);
    qos_manager.messageDelivered(1, 100, null, 100);
    qos_manager.messageDelivered(1, 100, null, 100);

    // Should not allow more consumption (reached prefetch limit)
    try std.testing.expect(!qos_manager.canConsume(1, 100));

    // Acknowledge one message
    qos_manager.messageAcknowledged(1, 100, null, 100);

    // Should allow consumption again
    try std.testing.expect(qos_manager.canConsume(1, 100));
}

test "qos manager global limits" {
    const allocator = std.testing.allocator;

    var qos_manager = QoSManager.init(allocator);
    defer qos_manager.deinit();

    // Set global QoS
    try qos_manager.setChannelQoS(1, 2, 1024, true);

    // Deliver messages on different channels
    qos_manager.messageDelivered(1, 100, null, 100);
    qos_manager.messageDelivered(2, 100, null, 100);

    // Should not allow more consumption (global limit reached)
    try std.testing.expect(!qos_manager.canConsume(1, 100));
    try std.testing.expect(!qos_manager.canConsume(2, 100));

    // Acknowledge message
    qos_manager.messageAcknowledged(1, 100, null, 100);

    // Should allow consumption again
    try std.testing.expect(qos_manager.canConsume(1, 100));
}

test "qos manager connection and channel lifecycle" {
    const allocator = std.testing.allocator;

    var qos_manager = QoSManager.init(allocator);
    defer qos_manager.deinit();

    // Add connection and channels
    try qos_manager.addConnection(100);
    try qos_manager.addChannel(100, 1);
    try qos_manager.addChannel(100, 2);

    // Set consumer QoS
    try qos_manager.setConsumerQoS("consumer1", 1, 100, 3, 1024);

    try std.testing.expect(qos_manager.canConsumeConsumer("consumer1"));

    // Remove channel (should also remove consumer QoS)
    qos_manager.removeChannel(100, 1);

    // Consumer should be removed
    try std.testing.expect(qos_manager.canConsumeConsumer("consumer1")); // No limits for unknown consumer

    // Remove connection
    qos_manager.removeConnection(100);
}

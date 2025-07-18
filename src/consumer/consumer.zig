const std = @import("std");
const Message = @import("../message.zig").Message;
const QoSManager = @import("../qos/qos.zig").QoSManager;

pub const Consumer = struct {
    tag: []const u8,
    queue_name: []const u8,
    channel_id: u16,
    connection_id: u64,
    no_ack: bool,
    exclusive: bool,
    no_local: bool,
    no_wait: bool,
    arguments: ?[]const u8,

    // QoS and flow control
    prefetch_count: u16,
    prefetch_size: u32,
    unacked_messages: u32,
    unacked_size: u32,

    // State management
    active: bool,
    cancelled: bool,

    // Statistics
    messages_delivered: u64,
    messages_acked: u64,
    messages_rejected: u64,
    bytes_delivered: u64,

    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        tag: []const u8,
        queue_name: []const u8,
        channel_id: u16,
        connection_id: u64,
        no_ack: bool,
        exclusive: bool,
        no_local: bool,
        no_wait: bool,
        arguments: ?[]const u8,
    ) !Consumer {
        return Consumer{
            .tag = try allocator.dupe(u8, tag),
            .queue_name = try allocator.dupe(u8, queue_name),
            .channel_id = channel_id,
            .connection_id = connection_id,
            .no_ack = no_ack,
            .exclusive = exclusive,
            .no_local = no_local,
            .no_wait = no_wait,
            .arguments = if (arguments) |args| try allocator.dupe(u8, args) else null,
            .prefetch_count = 0,
            .prefetch_size = 0,
            .unacked_messages = 0,
            .unacked_size = 0,
            .active = true,
            .cancelled = false,
            .messages_delivered = 0,
            .messages_acked = 0,
            .messages_rejected = 0,
            .bytes_delivered = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Consumer) void {
        self.allocator.free(self.tag);
        self.allocator.free(self.queue_name);
        if (self.arguments) |args| {
            self.allocator.free(args);
        }
    }

    pub fn canConsume(self: *const Consumer) bool {
        if (!self.active or self.cancelled) return false;

        // Check prefetch limits
        if (self.prefetch_count > 0 and self.unacked_messages >= self.prefetch_count) {
            return false;
        }

        if (self.prefetch_size > 0 and self.unacked_size >= self.prefetch_size) {
            return false;
        }

        return true;
    }

    pub fn canConsumeWithQoS(self: *const Consumer, qos_manager: *const QoSManager) bool {
        if (!self.canConsume()) return false;
        return qos_manager.canConsume(self.channel_id, self.connection_id);
    }

    pub fn deliverMessage(self: *Consumer, message: *const Message) void {
        if (!self.active or self.cancelled) return;

        // Update consumer statistics
        self.messages_delivered += 1;
        self.bytes_delivered += message.body.len;

        if (!self.no_ack) {
            self.unacked_messages += 1;
            self.unacked_size += @intCast(message.body.len);
        }

        std.log.debug("Message delivered to consumer {s}: {} bytes", .{ self.tag, message.body.len });
    }

    pub fn acknowledge(self: *Consumer, delivery_tag: u64, multiple: bool) void {
        if (multiple) {
            // For simplicity, just reset all unacked counters
            // In a real implementation, we'd track individual messages
            self.messages_acked += self.unacked_messages;
            self.unacked_messages = 0;
            self.unacked_size = 0;
        } else {
            if (self.unacked_messages > 0) {
                self.unacked_messages -= 1;
                self.messages_acked += 1;
                // In a real implementation, we'd subtract the actual message size
                self.unacked_size = if (self.unacked_size > 0) self.unacked_size - 1 else 0;
            }
        }

        std.log.debug("Message acknowledged by consumer {s}: delivery_tag={}, multiple={}", .{ self.tag, delivery_tag, multiple });
    }

    pub fn reject(self: *Consumer, delivery_tag: u64, requeue: bool) void {
        if (self.unacked_messages > 0) {
            self.unacked_messages -= 1;
            self.messages_rejected += 1;
            self.unacked_size = if (self.unacked_size > 0) self.unacked_size - 1 else 0;
        }

        std.log.debug("Message rejected by consumer {s}: delivery_tag={}, requeue={}", .{ self.tag, delivery_tag, requeue });
    }

    pub fn cancel(self: *Consumer) void {
        self.cancelled = true;
        self.active = false;
        std.log.debug("Consumer cancelled: {s}", .{self.tag});
    }

    pub fn setQoS(self: *Consumer, prefetch_count: u16, prefetch_size: u32) void {
        self.prefetch_count = prefetch_count;
        self.prefetch_size = prefetch_size;
        std.log.debug("QoS set for consumer {s}: prefetch_count={}, prefetch_size={}", .{ self.tag, prefetch_count, prefetch_size });
    }

    pub fn getDeliveryMode(self: *const Consumer) DeliveryMode {
        if (self.no_ack) {
            return .fire_and_forget;
        } else {
            return .at_least_once;
        }
    }

    pub fn getUnackedCount(self: *const Consumer) u32 {
        return self.unacked_messages;
    }

    pub fn getUnackedSize(self: *const Consumer) u32 {
        return self.unacked_size;
    }

    pub fn isExclusive(self: *const Consumer) bool {
        return self.exclusive;
    }

    pub fn getStats(self: *const Consumer, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("tag", std.json.Value{ .string = self.tag });
        try stats.put("queue_name", std.json.Value{ .string = self.queue_name });
        try stats.put("channel_id", std.json.Value{ .integer = @intCast(self.channel_id) });
        try stats.put("connection_id", std.json.Value{ .integer = @intCast(self.connection_id) });
        try stats.put("no_ack", std.json.Value{ .bool = self.no_ack });
        try stats.put("exclusive", std.json.Value{ .bool = self.exclusive });
        try stats.put("active", std.json.Value{ .bool = self.active });
        try stats.put("cancelled", std.json.Value{ .bool = self.cancelled });
        try stats.put("prefetch_count", std.json.Value{ .integer = @intCast(self.prefetch_count) });
        try stats.put("prefetch_size", std.json.Value{ .integer = @intCast(self.prefetch_size) });
        try stats.put("unacked_messages", std.json.Value{ .integer = @intCast(self.unacked_messages) });
        try stats.put("unacked_size", std.json.Value{ .integer = @intCast(self.unacked_size) });
        try stats.put("messages_delivered", std.json.Value{ .integer = @intCast(self.messages_delivered) });
        try stats.put("messages_acked", std.json.Value{ .integer = @intCast(self.messages_acked) });
        try stats.put("messages_rejected", std.json.Value{ .integer = @intCast(self.messages_rejected) });
        try stats.put("bytes_delivered", std.json.Value{ .integer = @intCast(self.bytes_delivered) });

        return std.json.Value{ .object = stats };
    }

    pub fn clone(self: *const Consumer, allocator: std.mem.Allocator) !Consumer {
        return Consumer.init(
            allocator,
            self.tag,
            self.queue_name,
            self.channel_id,
            self.connection_id,
            self.no_ack,
            self.exclusive,
            self.no_local,
            self.no_wait,
            self.arguments,
        );
    }
};

pub const DeliveryMode = enum {
    fire_and_forget,
    at_least_once,
    exactly_once, // Future enhancement
};

// Consumer registry for managing multiple consumers
pub const ConsumerRegistry = struct {
    consumers: std.HashMap([]const u8, *Consumer, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    consumers_by_queue: std.HashMap([]const u8, std.ArrayList(*Consumer), std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ConsumerRegistry {
        return ConsumerRegistry{
            .consumers = std.HashMap([]const u8, *Consumer, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .consumers_by_queue = std.HashMap([]const u8, std.ArrayList(*Consumer), std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConsumerRegistry) void {
        // Clean up consumers
        var consumer_iterator = self.consumers.iterator();
        while (consumer_iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.consumers.deinit();

        // Clean up queue consumer lists
        var queue_iterator = self.consumers_by_queue.iterator();
        while (queue_iterator.next()) |entry| {
            entry.value_ptr.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.consumers_by_queue.deinit();
    }

    pub fn addConsumer(self: *ConsumerRegistry, consumer: Consumer) !void {
        // Check for duplicate consumer tag
        if (self.consumers.contains(consumer.tag)) {
            return error.ConsumerTagInUse;
        }

        // Store consumer
        const owned_tag = try self.allocator.dupe(u8, consumer.tag);
        const consumer_ptr = try self.allocator.create(Consumer);
        consumer_ptr.* = consumer;

        try self.consumers.put(owned_tag, consumer_ptr);

        // Add to queue consumer list
        const result = try self.consumers_by_queue.getOrPut(consumer.queue_name);
        if (!result.found_existing) {
            result.value_ptr.* = std.ArrayList(*Consumer).init(self.allocator);
        }
        try result.value_ptr.append(consumer_ptr);

        std.log.debug("Consumer registered: {s} -> {s}", .{ consumer.tag, consumer.queue_name });
    }

    pub fn removeConsumer(self: *ConsumerRegistry, consumer_tag: []const u8) !void {
        const consumer = self.consumers.get(consumer_tag) orelse return error.ConsumerNotFound;

        // Remove from queue consumer list
        if (self.consumers_by_queue.getPtr(consumer.queue_name)) |queue_consumers| {
            for (queue_consumers.items, 0..) |queue_consumer, i| {
                if (queue_consumer == consumer) {
                    _ = queue_consumers.swapRemove(i);
                    break;
                }
            }

            // Remove empty queue consumer list
            if (queue_consumers.items.len == 0) {
                var removed_entry = self.consumers_by_queue.fetchRemove(consumer.queue_name).?;
                removed_entry.value.deinit();
                self.allocator.free(removed_entry.key);
            }
        }

        // Remove consumer
        if (self.consumers.fetchRemove(consumer_tag)) |entry| {
            entry.value.deinit();
            self.allocator.destroy(entry.value);
            self.allocator.free(entry.key);
        }

        std.log.debug("Consumer unregistered: {s}", .{consumer_tag});
    }

    pub fn getConsumer(self: *ConsumerRegistry, consumer_tag: []const u8) ?*Consumer {
        return self.consumers.get(consumer_tag);
    }

    pub fn getConsumersForQueue(self: *ConsumerRegistry, queue_name: []const u8) ?[]const *Consumer {
        if (self.consumers_by_queue.get(queue_name)) |queue_consumers| {
            return queue_consumers.items;
        }
        return null;
    }

    pub fn getConsumerCount(self: *const ConsumerRegistry) u32 {
        return @intCast(self.consumers.count());
    }

    pub fn getQueueConsumerCount(self: *const ConsumerRegistry, queue_name: []const u8) u32 {
        if (self.consumers_by_queue.get(queue_name)) |queue_consumers| {
            return @intCast(queue_consumers.items.len);
        }
        return 0;
    }

    pub fn cancelAll(self: *ConsumerRegistry) void {
        var iterator = self.consumers.valueIterator();
        while (iterator.next()) |consumer| {
            consumer.*.cancel();
        }
    }

    pub fn getAllStats(self: *const ConsumerRegistry, allocator: std.mem.Allocator) !std.json.Value {
        var stats_array = std.json.Array.init(allocator);

        var iterator = self.consumers.valueIterator();
        while (iterator.next()) |consumer| {
            const consumer_stats = try consumer.*.getStats(allocator);
            try stats_array.append(consumer_stats);
        }

        return std.json.Value{ .array = stats_array };
    }
};

test "consumer creation and basic operations" {
    const allocator = std.testing.allocator;

    var consumer = try Consumer.init(
        allocator,
        "test-consumer",
        "test-queue",
        1,
        100,
        false,
        false,
        false,
        false,
        null,
    );
    defer consumer.deinit();

    try std.testing.expectEqualStrings("test-consumer", consumer.tag);
    try std.testing.expectEqualStrings("test-queue", consumer.queue_name);
    try std.testing.expectEqual(@as(u16, 1), consumer.channel_id);
    try std.testing.expectEqual(@as(u64, 100), consumer.connection_id);
    try std.testing.expectEqual(true, consumer.active);
    try std.testing.expectEqual(false, consumer.cancelled);

    // Test message delivery
    var message = try Message.init(allocator, 1, "test.exchange", "test.key", "Hello, World!");
    defer message.deinit();

    try std.testing.expect(consumer.canConsume());
    consumer.deliverMessage(&message);
    try std.testing.expectEqual(@as(u32, 1), consumer.unacked_messages);

    // Test acknowledgment
    consumer.acknowledge(1, false);
    try std.testing.expectEqual(@as(u32, 0), consumer.unacked_messages);
    try std.testing.expectEqual(@as(u64, 1), consumer.messages_acked);

    // Test cancellation
    consumer.cancel();
    try std.testing.expectEqual(false, consumer.active);
    try std.testing.expectEqual(true, consumer.cancelled);
}

test "consumer registry operations" {
    const allocator = std.testing.allocator;

    var registry = ConsumerRegistry.init(allocator);
    defer registry.deinit();

    try std.testing.expectEqual(@as(u32, 0), registry.getConsumerCount());

    // Add consumers
    const consumer1 = try Consumer.init(allocator, "consumer1", "queue1", 1, 100, false, false, false, false, null);
    const consumer2 = try Consumer.init(allocator, "consumer2", "queue1", 1, 100, false, false, false, false, null);
    const consumer3 = try Consumer.init(allocator, "consumer3", "queue2", 2, 101, false, false, false, false, null);

    try registry.addConsumer(consumer1);
    try registry.addConsumer(consumer2);
    try registry.addConsumer(consumer3);

    try std.testing.expectEqual(@as(u32, 3), registry.getConsumerCount());
    try std.testing.expectEqual(@as(u32, 2), registry.getQueueConsumerCount("queue1"));
    try std.testing.expectEqual(@as(u32, 1), registry.getQueueConsumerCount("queue2"));

    // Remove consumer
    try registry.removeConsumer("consumer1");
    try std.testing.expectEqual(@as(u32, 2), registry.getConsumerCount());
    try std.testing.expectEqual(@as(u32, 1), registry.getQueueConsumerCount("queue1"));
}

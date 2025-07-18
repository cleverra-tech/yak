const std = @import("std");
const Message = @import("../message.zig").Message;
const Consumer = @import("../consumer/consumer.zig").Consumer;
const Binding = @import("binding.zig").Binding;

pub const Queue = struct {
    name: []const u8,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    arguments: ?[]const u8,

    // Message storage
    messages: std.ArrayList(Message),
    max_length: ?u32,
    message_ttl: ?u32,

    // Consumer management
    consumers: std.ArrayList(Consumer),
    next_delivery_tag: u64,

    // Bindings
    bindings: std.ArrayList(Binding),

    // Statistics
    messages_total: u64,
    messages_ready: u32,
    messages_unacknowledged: u32,
    consumers_total: u64,
    memory_usage: u64,

    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        arguments: ?[]const u8,
    ) !Queue {
        return Queue{
            .name = name,
            .durable = durable,
            .exclusive = exclusive,
            .auto_delete = auto_delete,
            .arguments = if (arguments) |args| try allocator.dupe(u8, args) else null,
            .messages = std.ArrayList(Message).init(allocator),
            .max_length = null,
            .message_ttl = null,
            .consumers = std.ArrayList(Consumer).init(allocator),
            .next_delivery_tag = 1,
            .bindings = std.ArrayList(Binding).init(allocator),
            .messages_total = 0,
            .messages_ready = 0,
            .messages_unacknowledged = 0,
            .consumers_total = 0,
            .memory_usage = 0,
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Queue) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Clean up messages
        for (self.messages.items) |*message| {
            message.deinit();
        }
        self.messages.deinit();

        // Clean up consumers
        for (self.consumers.items) |*consumer| {
            consumer.deinit();
        }
        self.consumers.deinit();

        // Clean up bindings
        for (self.bindings.items) |*binding| {
            binding.deinit();
        }
        self.bindings.deinit();

        if (self.arguments) |args| {
            self.allocator.free(args);
        }
    }

    pub fn publish(self: *Queue, message: Message) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check queue length limits
        if (self.max_length) |max_len| {
            if (self.messages.items.len >= max_len) {
                // Drop oldest message (head drop policy)
                if (self.messages.items.len > 0) {
                    var dropped = self.messages.orderedRemove(0);
                    dropped.deinit();
                    self.messages_ready = @max(1, self.messages_ready) - 1;
                }
            }
        }

        try self.messages.append(message);
        self.messages_total += 1;
        self.messages_ready += 1;
        self.memory_usage += message.body.len;

        std.log.debug("Message published to queue {s}: {} bytes", .{ self.name, message.body.len });

        // Try to deliver to waiting consumers
        self.tryDeliverMessages();
    }

    pub fn consume(self: *Queue, consumer: Consumer) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check if queue is exclusive and already has consumers
        if (self.exclusive and self.consumers.items.len > 0) {
            return error.QueueExclusive;
        }

        // Check for duplicate consumer tags
        for (self.consumers.items) |existing_consumer| {
            if (std.mem.eql(u8, existing_consumer.tag, consumer.tag)) {
                return error.ConsumerTagInUse;
            }
        }

        try self.consumers.append(consumer);
        self.consumers_total += 1;

        std.log.debug("Consumer added to queue {s}: {s}", .{ self.name, consumer.tag });

        // Try to deliver existing messages
        self.tryDeliverMessages();
    }

    pub fn cancel(self: *Queue, consumer_tag: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.consumers.items, 0..) |consumer, i| {
            if (std.mem.eql(u8, consumer.tag, consumer_tag)) {
                var removed_consumer = self.consumers.swapRemove(i);
                removed_consumer.deinit();
                std.log.debug("Consumer cancelled from queue {s}: {s}", .{ self.name, consumer_tag });
                return;
            }
        }
        return error.ConsumerNotFound;
    }

    pub fn acknowledge(self: *Queue, delivery_tag: u64, multiple: bool) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (multiple) {
            // Acknowledge all messages up to and including delivery_tag
            var i: usize = 0;
            while (i < self.messages.items.len) {
                if (self.messages.items[i].id <= delivery_tag) {
                    var acked_message = self.messages.orderedRemove(i);
                    acked_message.deinit();
                    self.messages_unacknowledged = @max(1, self.messages_unacknowledged) - 1;
                    self.memory_usage -= acked_message.body.len;
                } else {
                    i += 1;
                }
            }
        } else {
            // Acknowledge single message
            for (self.messages.items, 0..) |message, i| {
                if (message.id == delivery_tag) {
                    var acked_message = self.messages.orderedRemove(i);
                    acked_message.deinit();
                    self.messages_unacknowledged = @max(1, self.messages_unacknowledged) - 1;
                    self.memory_usage -= acked_message.body.len;
                    break;
                }
            }
        }

        std.log.debug("Message(s) acknowledged in queue {s}: delivery_tag={}", .{ self.name, delivery_tag });
    }

    pub fn reject(self: *Queue, delivery_tag: u64, requeue: bool) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.messages.items, 0..) |*message, i| {
            if (message.id == delivery_tag) {
                if (requeue) {
                    // Move message back to ready state
                    // In a more sophisticated implementation, we might move it to the end of the queue
                    message.incrementDeliveryCount();
                    self.messages_unacknowledged = @max(1, self.messages_unacknowledged) - 1;
                    self.messages_ready += 1;
                    std.log.debug("Message requeued in queue {s}: delivery_tag={}", .{ self.name, delivery_tag });
                } else {
                    // Drop the message
                    var rejected_message = self.messages.orderedRemove(i);
                    rejected_message.deinit();
                    self.messages_unacknowledged = @max(1, self.messages_unacknowledged) - 1;
                    self.memory_usage -= rejected_message.body.len;
                    std.log.debug("Message rejected and dropped from queue {s}: delivery_tag={}", .{ self.name, delivery_tag });
                }
                return;
            }
        }
        return error.MessageNotFound;
    }

    pub fn get(self: *Queue, no_ack: bool) !?Message {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.messages.items.len == 0) {
            return null;
        }

        var message = self.messages.orderedRemove(0);
        self.messages_ready = @max(1, self.messages_ready) - 1;

        if (!no_ack) {
            message.id = self.next_delivery_tag;
            self.next_delivery_tag += 1;
            self.messages_unacknowledged += 1;
            // In a real implementation, we'd keep the message for potential redelivery
        } else {
            self.memory_usage -= message.body.len;
        }

        std.log.debug("Message retrieved from queue {s}: {} bytes (no_ack={})", .{ self.name, message.body.len, no_ack });
        return message;
    }

    pub fn purge(self: *Queue) u32 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const message_count = @as(u32, @intCast(self.messages.items.len));

        for (self.messages.items) |*message| {
            message.deinit();
        }
        self.messages.clearAndFree();

        self.messages_ready = 0;
        self.messages_unacknowledged = 0;
        self.memory_usage = 0;

        std.log.debug("Queue purged: {s} ({} messages)", .{ self.name, message_count });
        return message_count;
    }

    pub fn addBinding(
        self: *Queue,
        exchange_name: []const u8,
        routing_key: []const u8,
        arguments: ?[]const u8,
    ) !void {
        const binding = try Binding.init(
            self.allocator,
            self.name,
            exchange_name,
            routing_key,
            arguments,
        );
        try self.bindings.append(binding);
    }

    pub fn removeBinding(
        self: *Queue,
        exchange_name: []const u8,
        routing_key: []const u8,
        arguments: ?[]const u8,
    ) !void {
        for (self.bindings.items, 0..) |binding, i| {
            if (std.mem.eql(u8, binding.exchange_name, exchange_name) and
                std.mem.eql(u8, binding.routing_key, routing_key) and
                binding.argumentsMatch(arguments))
            {
                var removed_binding = self.bindings.swapRemove(i);
                removed_binding.deinit();
                return;
            }
        }
        return error.BindingNotFound;
    }

    pub fn removeBindingsForExchange(self: *Queue, exchange_name: []const u8) void {
        var i: usize = 0;
        while (i < self.bindings.items.len) {
            if (std.mem.eql(u8, self.bindings.items[i].exchange_name, exchange_name)) {
                var removed_binding = self.bindings.swapRemove(i);
                removed_binding.deinit();
            } else {
                i += 1;
            }
        }
    }

    fn tryDeliverMessages(self: *Queue) void {
        // Round-robin delivery to consumers
        var consumer_index: usize = 0;
        var message_index: usize = 0;

        while (message_index < self.messages.items.len and self.consumers.items.len > 0) {
            const consumer = &self.consumers.items[consumer_index % self.consumers.items.len];

            // Check if consumer can accept more messages (QoS limits)
            if (consumer.canConsume()) {
                var message = self.messages.orderedRemove(message_index);
                message.id = self.next_delivery_tag;
                self.next_delivery_tag += 1;

                // Deliver message to consumer
                consumer.deliverMessage(&message);

                self.messages_ready = @max(1, self.messages_ready) - 1;
                if (!consumer.no_ack) {
                    self.messages_unacknowledged += 1;
                } else {
                    message.deinit();
                    self.memory_usage -= message.body.len;
                }

                std.log.debug("Message delivered to consumer {s} from queue {s}", .{ consumer.tag, self.name });
            } else {
                message_index += 1;
            }

            consumer_index += 1;
        }
    }

    pub fn getMessageCount(self: *const Queue) u32 {
        return @intCast(self.messages.items.len);
    }

    pub fn getConsumerCount(self: *const Queue) u32 {
        return @intCast(self.consumers.items.len);
    }

    pub fn isEmpty(self: *const Queue) bool {
        return self.messages.items.len == 0;
    }

    pub fn shouldAutoDelete(self: *const Queue) bool {
        return self.auto_delete and self.consumers.items.len == 0;
    }

    pub fn getStats(self: *const Queue, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("name", std.json.Value{ .string = self.name });
        try stats.put("durable", std.json.Value{ .bool = self.durable });
        try stats.put("exclusive", std.json.Value{ .bool = self.exclusive });
        try stats.put("auto_delete", std.json.Value{ .bool = self.auto_delete });
        try stats.put("messages_ready", std.json.Value{ .integer = @intCast(self.messages_ready) });
        try stats.put("messages_unacknowledged", std.json.Value{ .integer = @intCast(self.messages_unacknowledged) });
        try stats.put("messages_total", std.json.Value{ .integer = @intCast(self.messages_total) });
        try stats.put("consumers", std.json.Value{ .integer = @intCast(self.consumers.items.len) });
        try stats.put("consumers_total", std.json.Value{ .integer = @intCast(self.consumers_total) });
        try stats.put("memory_usage", std.json.Value{ .integer = @intCast(self.memory_usage) });
        try stats.put("bindings", std.json.Value{ .integer = @intCast(self.bindings.items.len) });

        return std.json.Value{ .object = stats };
    }
};

test "queue creation and basic operations" {
    const allocator = std.testing.allocator;

    var queue = try Queue.init(allocator, "test.queue", true, false, false, null);
    defer queue.deinit();

    try std.testing.expectEqualStrings("test.queue", queue.name);
    try std.testing.expectEqual(true, queue.durable);
    try std.testing.expectEqual(@as(u32, 0), queue.getMessageCount());
    try std.testing.expectEqual(@as(u32, 0), queue.getConsumerCount());

    // Publish a message
    const message = try Message.init(allocator, 1, "test.exchange", "test.key", "Hello, World!");
    try queue.publish(message);

    try std.testing.expectEqual(@as(u32, 1), queue.getMessageCount());

    // Get the message
    const retrieved = try queue.get(true);
    try std.testing.expect(retrieved != null);
    if (retrieved) |msg| {
        try std.testing.expectEqualStrings("Hello, World!", msg.body);
    }

    try std.testing.expectEqual(@as(u32, 0), queue.getMessageCount());
}

test "queue purge" {
    const allocator = std.testing.allocator;

    var queue = try Queue.init(allocator, "test.queue", true, false, false, null);
    defer queue.deinit();

    // Publish multiple messages
    for (0..5) |i| {
        const message = try Message.init(allocator, i, "test.exchange", "test.key", "test message");
        try queue.publish(message);
    }

    try std.testing.expectEqual(@as(u32, 5), queue.getMessageCount());

    const purged_count = queue.purge();
    try std.testing.expectEqual(@as(u32, 5), purged_count);
    try std.testing.expectEqual(@as(u32, 0), queue.getMessageCount());
}

test "queue binding arguments matching" {
    const allocator = std.testing.allocator;

    var queue = try Queue.init(allocator, "test.queue", true, false, false, null);
    defer queue.deinit();

    // Add bindings with different arguments
    try queue.addBinding("exchange1", "key1", null);
    try queue.addBinding("exchange1", "key1", "args1");
    try queue.addBinding("exchange1", "key1", "args2");

    try std.testing.expectEqual(@as(usize, 3), queue.bindings.items.len);

    // Remove binding with null arguments should only remove the null binding
    try queue.removeBinding("exchange1", "key1", null);
    try std.testing.expectEqual(@as(usize, 2), queue.bindings.items.len);

    // Remove binding with specific arguments should only remove matching binding
    try queue.removeBinding("exchange1", "key1", "args1");
    try std.testing.expectEqual(@as(usize, 1), queue.bindings.items.len);

    // Trying to remove with wrong arguments should fail
    const result = queue.removeBinding("exchange1", "key1", "wrong_args");
    try std.testing.expectError(error.BindingNotFound, result);
    try std.testing.expectEqual(@as(usize, 1), queue.bindings.items.len);

    // Remove with correct arguments should succeed
    try queue.removeBinding("exchange1", "key1", "args2");
    try std.testing.expectEqual(@as(usize, 0), queue.bindings.items.len);
}

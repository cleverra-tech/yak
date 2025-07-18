const std = @import("std");
const Exchange = @import("../routing/exchange.zig").Exchange;
const ExchangeType = @import("../routing/exchange.zig").ExchangeType;
const Queue = @import("../routing/queue.zig").Queue;

pub const VirtualHost = struct {
    name: []const u8,
    exchanges: std.HashMap([]const u8, *Exchange, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    queues: std.HashMap([]const u8, *Queue, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    allocator: std.mem.Allocator,
    active: bool,

    pub fn init(allocator: std.mem.Allocator, name: []const u8) !VirtualHost {
        var vhost = VirtualHost{
            .name = name,
            .exchanges = std.HashMap([]const u8, *Exchange, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .queues = std.HashMap([]const u8, *Queue, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
            .active = true,
        };

        // Create default exchanges
        try vhost.createDefaultExchanges();

        return vhost;
    }

    pub fn deinit(self: *VirtualHost) void {
        // Clean up exchanges
        var exchange_iterator = self.exchanges.iterator();
        while (exchange_iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.exchanges.deinit();

        // Clean up queues
        var queue_iterator = self.queues.iterator();
        while (queue_iterator.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.queues.deinit();
    }

    fn createDefaultExchanges(self: *VirtualHost) !void {
        // Default direct exchange (empty name)
        try self.declareExchange("", .direct, true, false, false, null);

        // amq.direct
        try self.declareExchange("amq.direct", .direct, true, false, false, null);

        // amq.fanout
        try self.declareExchange("amq.fanout", .fanout, true, false, false, null);

        // amq.topic
        try self.declareExchange("amq.topic", .topic, true, false, false, null);

        // amq.headers
        try self.declareExchange("amq.headers", .headers, true, false, false, null);
    }

    pub fn declareExchange(
        self: *VirtualHost,
        name: []const u8,
        exchange_type: ExchangeType,
        durable: bool,
        auto_delete: bool,
        internal: bool,
        arguments: ?[]const u8,
    ) !void {
        if (self.exchanges.contains(name)) {
            // Exchange already exists, check if parameters match
            const existing = self.exchanges.get(name).?;
            if (existing.exchange_type != exchange_type or
                existing.durable != durable or
                existing.auto_delete != auto_delete or
                existing.internal != internal)
            {
                return error.ExchangeParameterMismatch;
            }
            return; // Exchange exists with same parameters
        }

        const owned_name = try self.allocator.dupe(u8, name);
        const exchange = try self.allocator.create(Exchange);
        exchange.* = try Exchange.init(self.allocator, owned_name, exchange_type, durable, auto_delete, internal, arguments);

        try self.exchanges.put(owned_name, exchange);
        std.log.info("Exchange declared: {s} (type: {s}, durable: {})", .{ name, @tagName(exchange_type), durable });
    }

    pub fn deleteExchange(self: *VirtualHost, name: []const u8, if_unused: bool) !void {
        const exchange = self.exchanges.get(name) orelse return error.ExchangeNotFound;

        if (if_unused and exchange.bindings.items.len > 0) {
            return error.ExchangeInUse;
        }

        // Remove all bindings for this exchange
        var queue_iterator = self.queues.iterator();
        while (queue_iterator.next()) |entry| {
            entry.value_ptr.*.removeBindingsForExchange(name);
        }

        if (self.exchanges.fetchRemove(name)) |entry| {
            entry.value.deinit();
            self.allocator.destroy(entry.value);
            self.allocator.free(entry.key);
        }

        std.log.info("Exchange deleted: {s}", .{name});
    }

    pub fn declareQueue(
        self: *VirtualHost,
        name: []const u8,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        arguments: ?[]const u8,
    ) ![]const u8 {
        var queue_name = name;

        // Generate name for anonymous queues
        if (name.len == 0) {
            queue_name = try self.generateQueueName();
        }

        if (self.queues.contains(queue_name)) {
            const existing = self.queues.get(queue_name).?;
            if (existing.durable != durable or
                existing.exclusive != exclusive or
                existing.auto_delete != auto_delete)
            {
                return error.QueueParameterMismatch;
            }
            return queue_name; // Queue exists with same parameters
        }

        const owned_name = try self.allocator.dupe(u8, queue_name);
        const queue = try self.allocator.create(Queue);
        queue.* = try Queue.init(self.allocator, owned_name, durable, exclusive, auto_delete, arguments);

        try self.queues.put(owned_name, queue);
        std.log.info("Queue declared: {s} (durable: {}, exclusive: {}, auto_delete: {})", .{ queue_name, durable, exclusive, auto_delete });

        return owned_name;
    }

    pub fn deleteQueue(self: *VirtualHost, name: []const u8, if_unused: bool, if_empty: bool) !u32 {
        const queue = self.queues.get(name) orelse return error.QueueNotFound;

        if (if_unused and queue.consumers.items.len > 0) {
            return error.QueueInUse;
        }

        if (if_empty and queue.getMessageCount() > 0) {
            return error.QueueNotEmpty;
        }

        const message_count = queue.getMessageCount();

        // Remove all bindings for this queue
        var exchange_iterator = self.exchanges.iterator();
        while (exchange_iterator.next()) |entry| {
            entry.value_ptr.*.removeBindingsForQueue(name);
        }

        if (self.queues.fetchRemove(name)) |entry| {
            entry.value.deinit();
            self.allocator.destroy(entry.value);
            self.allocator.free(entry.key);
        }

        std.log.info("Queue deleted: {s} (had {} messages)", .{ name, message_count });
        return message_count;
    }

    pub fn bindQueue(
        self: *VirtualHost,
        queue_name: []const u8,
        exchange_name: []const u8,
        routing_key: []const u8,
        arguments: ?[]const u8,
    ) !void {
        const queue = self.queues.get(queue_name) orelse return error.QueueNotFound;
        const exchange = self.exchanges.get(exchange_name) orelse return error.ExchangeNotFound;

        try exchange.bindQueue(queue_name, routing_key, arguments);
        try queue.addBinding(exchange_name, routing_key, arguments);

        std.log.info("Queue bound: {s} -> {s} (key: {s})", .{ exchange_name, queue_name, routing_key });
    }

    pub fn unbindQueue(
        self: *VirtualHost,
        queue_name: []const u8,
        exchange_name: []const u8,
        routing_key: []const u8,
        arguments: ?[]const u8,
    ) !void {
        const queue = self.queues.get(queue_name) orelse return error.QueueNotFound;
        const exchange = self.exchanges.get(exchange_name) orelse return error.ExchangeNotFound;

        try exchange.unbindQueue(queue_name, routing_key, arguments);
        try queue.removeBinding(exchange_name, routing_key, arguments);

        std.log.info("Queue unbound: {s} -/-> {s} (key: {s})", .{ exchange_name, queue_name, routing_key });
    }

    pub fn getExchange(self: *VirtualHost, name: []const u8) ?*Exchange {
        return self.exchanges.get(name);
    }

    pub fn getQueue(self: *VirtualHost, name: []const u8) ?*Queue {
        return self.queues.get(name);
    }

    pub fn getExchangeCount(self: *const VirtualHost) u32 {
        return @intCast(self.exchanges.count());
    }

    pub fn getQueueCount(self: *const VirtualHost) u32 {
        return @intCast(self.queues.count());
    }

    pub fn listQueues(self: *VirtualHost, allocator: std.mem.Allocator) ![][]const u8 {
        var queue_list = std.ArrayList([]const u8).init(allocator);
        defer queue_list.deinit();

        var iterator = self.queues.iterator();
        while (iterator.next()) |entry| {
            try queue_list.append(try allocator.dupe(u8, entry.key_ptr.*));
        }

        return queue_list.toOwnedSlice();
    }

    pub fn purgeQueue(self: *VirtualHost, name: []const u8) !u32 {
        const queue = self.queues.get(name) orelse return error.QueueNotFound;
        const message_count = queue.purge();
        std.log.info("Queue purged: {s} ({} messages removed)", .{ name, message_count });
        return message_count;
    }

    fn generateQueueName(self: *VirtualHost) ![]const u8 {
        var random = std.Random.DefaultPrng.init(@intCast(std.time.timestamp()));
        const rand_int = random.random().int(u32);
        return try std.fmt.allocPrint(self.allocator, "amq.gen-{x}", .{rand_int});
    }

    // Statistics and monitoring
    pub fn getStats(self: *const VirtualHost, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("name", std.json.Value{ .string = self.name });
        try stats.put("exchanges", std.json.Value{ .integer = @intCast(self.exchanges.count()) });
        try stats.put("queues", std.json.Value{ .integer = @intCast(self.queues.count()) });
        try stats.put("active", std.json.Value{ .bool = self.active });

        // Queue statistics
        var total_messages: u64 = 0;
        var total_consumers: u64 = 0;

        var queue_iterator = self.queues.valueIterator();
        while (queue_iterator.next()) |queue| {
            total_messages += queue.*.getMessageCount();
            total_consumers += queue.*.consumers.items.len;
        }

        try stats.put("total_messages", std.json.Value{ .integer = @intCast(total_messages) });
        try stats.put("total_consumers", std.json.Value{ .integer = @intCast(total_consumers) });

        return std.json.Value{ .object = stats };
    }
};

test "virtual host creation and management" {
    const allocator = std.testing.allocator;

    var vhost = try VirtualHost.init(allocator, "test-vhost");
    defer vhost.deinit();

    // Should have default exchanges
    try std.testing.expect(vhost.getExchangeCount() > 0);
    try std.testing.expectEqual(@as(u32, 0), vhost.getQueueCount());

    // Declare a queue
    const queue_name = try vhost.declareQueue("test-queue", true, false, false, null);
    try std.testing.expectEqualStrings("test-queue", queue_name);
    try std.testing.expectEqual(@as(u32, 1), vhost.getQueueCount());

    // Declare an exchange
    try vhost.declareExchange("test-exchange", .direct, true, false, false, null);
    try std.testing.expect(vhost.getExchange("test-exchange") != null);

    // Bind queue to exchange
    try vhost.bindQueue("test-queue", "test-exchange", "test-key", null);

    // Unbind queue
    try vhost.unbindQueue("test-queue", "test-exchange", "test-key", null);

    // Delete queue
    const deleted_message_count = try vhost.deleteQueue("test-queue", false, false);
    try std.testing.expectEqual(@as(u32, 0), deleted_message_count);
    try std.testing.expectEqual(@as(u32, 0), vhost.getQueueCount());
}

test "vhost queue listing" {
    const allocator = std.testing.allocator;

    var vhost = try VirtualHost.init(allocator, "/");
    defer vhost.deinit();

    // Initially no queues
    var queue_list = try vhost.listQueues(allocator);
    defer {
        for (queue_list) |name| {
            allocator.free(name);
        }
        allocator.free(queue_list);
    }
    try std.testing.expectEqual(@as(usize, 0), queue_list.len);

    // Create some queues
    _ = try vhost.declareQueue("queue1", true, false, false, null);
    _ = try vhost.declareQueue("queue2", false, true, false, null);
    _ = try vhost.declareQueue("queue3", true, false, true, null);

    // List queues again
    queue_list = try vhost.listQueues(allocator);
    defer {
        for (queue_list) |name| {
            allocator.free(name);
        }
        allocator.free(queue_list);
    }
    try std.testing.expectEqual(@as(usize, 3), queue_list.len);

    // Verify queue names are present (order might vary)
    var found_queue1 = false;
    var found_queue2 = false;
    var found_queue3 = false;

    for (queue_list) |name| {
        if (std.mem.eql(u8, name, "queue1")) found_queue1 = true;
        if (std.mem.eql(u8, name, "queue2")) found_queue2 = true;
        if (std.mem.eql(u8, name, "queue3")) found_queue3 = true;
    }

    try std.testing.expect(found_queue1);
    try std.testing.expect(found_queue2);
    try std.testing.expect(found_queue3);
}

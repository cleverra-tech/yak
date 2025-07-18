const std = @import("std");
const Message = @import("../message.zig").Message;
const HeaderTable = @import("../message.zig").HeaderTable;
const Binding = @import("binding.zig").Binding;

pub const ExchangeType = enum {
    direct,
    fanout,
    topic,
    headers,

    pub fn jsonStringify(self: ExchangeType, writer: anytype) !void {
        try writer.write("\"");
        try writer.write(@tagName(self));
        try writer.write("\"");
    }
};

pub const Exchange = struct {
    name: []const u8,
    exchange_type: ExchangeType,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    bindings: std.ArrayList(Binding),
    arguments: ?[]const u8,
    allocator: std.mem.Allocator,

    // Statistics
    messages_in: u64,
    messages_out: u64,
    bytes_in: u64,
    bytes_out: u64,

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        exchange_type: ExchangeType,
        durable: bool,
        auto_delete: bool,
        internal: bool,
        arguments: ?[]const u8,
    ) !Exchange {
        return Exchange{
            .name = name,
            .exchange_type = exchange_type,
            .durable = durable,
            .auto_delete = auto_delete,
            .internal = internal,
            .bindings = std.ArrayList(Binding).init(allocator),
            .arguments = if (arguments) |args| try allocator.dupe(u8, args) else null,
            .allocator = allocator,
            .messages_in = 0,
            .messages_out = 0,
            .bytes_in = 0,
            .bytes_out = 0,
        };
    }

    pub fn deinit(self: *Exchange) void {
        for (self.bindings.items) |*binding| {
            binding.deinit();
        }
        self.bindings.deinit();

        if (self.arguments) |args| {
            self.allocator.free(args);
        }
    }

    pub fn bindQueue(
        self: *Exchange,
        queue_name: []const u8,
        routing_key: []const u8,
        arguments: ?[]const u8,
    ) !void {
        // Check if binding already exists
        for (self.bindings.items) |binding| {
            if (std.mem.eql(u8, binding.queue_name, queue_name) and
                std.mem.eql(u8, binding.routing_key, routing_key))
            {
                return; // Binding already exists
            }
        }

        const binding = try Binding.init(
            self.allocator,
            queue_name,
            self.name,
            routing_key,
            arguments,
        );

        try self.bindings.append(binding);
        std.log.debug("Binding added: {s} -> {s} (key: {s})", .{ self.name, queue_name, routing_key });
    }

    pub fn unbindQueue(
        self: *Exchange,
        queue_name: []const u8,
        routing_key: []const u8,
        arguments: ?[]const u8,
    ) !void {
        for (self.bindings.items, 0..) |binding, i| {
            if (std.mem.eql(u8, binding.queue_name, queue_name) and
                std.mem.eql(u8, binding.routing_key, routing_key) and
                binding.argumentsMatch(arguments))
            {
                var removed_binding = self.bindings.swapRemove(i);
                removed_binding.deinit();
                std.log.debug("Binding removed: {s} -/-> {s} (key: {s})", .{ self.name, queue_name, routing_key });
                return;
            }
        }
        return error.BindingNotFound;
    }

    pub fn removeBindingsForQueue(self: *Exchange, queue_name: []const u8) void {
        var i: usize = 0;
        while (i < self.bindings.items.len) {
            if (std.mem.eql(u8, self.bindings.items[i].queue_name, queue_name)) {
                var removed_binding = self.bindings.swapRemove(i);
                removed_binding.deinit();
            } else {
                i += 1;
            }
        }
    }

    pub fn routeMessage(self: *Exchange, message: *const Message, allocator: std.mem.Allocator) ![][]const u8 {
        var matched_queues = std.ArrayList([]const u8).init(allocator);
        defer matched_queues.deinit();

        switch (self.exchange_type) {
            .direct => try self.routeDirect(message, &matched_queues),
            .fanout => try self.routeFanout(message, &matched_queues),
            .topic => try self.routeTopic(message, &matched_queues),
            .headers => try self.routeHeaders(message, &matched_queues),
        }

        // Update statistics
        self.messages_in += 1;
        self.bytes_in += message.body.len;
        self.messages_out += matched_queues.items.len;
        self.bytes_out += message.body.len * matched_queues.items.len;

        return matched_queues.toOwnedSlice();
    }

    fn routeDirect(self: *Exchange, message: *const Message, matched_queues: *std.ArrayList([]const u8)) !void {
        for (self.bindings.items) |binding| {
            if (std.mem.eql(u8, binding.routing_key, message.routing_key)) {
                try matched_queues.append(binding.queue_name);
            }
        }
    }

    fn routeFanout(self: *Exchange, message: *const Message, matched_queues: *std.ArrayList([]const u8)) !void {
        _ = message; // Fanout ignores routing key
        for (self.bindings.items) |binding| {
            try matched_queues.append(binding.queue_name);
        }
    }

    fn routeTopic(self: *Exchange, message: *const Message, matched_queues: *std.ArrayList([]const u8)) !void {
        for (self.bindings.items) |binding| {
            if (self.matchTopicPattern(binding.routing_key, message.routing_key)) {
                try matched_queues.append(binding.queue_name);
            }
        }
    }

    fn routeHeaders(self: *Exchange, message: *const Message, matched_queues: *std.ArrayList([]const u8)) !void {
        for (self.bindings.items) |binding| {
            if (self.matchHeaders(binding.arguments, message.headers)) {
                try matched_queues.append(binding.queue_name);
            }
        }
    }

    fn matchTopicPattern(self: *Exchange, pattern: []const u8, routing_key: []const u8) bool {
        _ = self;
        return matchTopicWildcards(pattern, routing_key);
    }

    fn matchHeaders(self: *Exchange, binding_args: ?[]const u8, message_headers: ?@import("../message.zig").HeaderTable) bool {

        // If no binding arguments, match everything (similar to fanout behavior)
        const args = binding_args orelse return true;

        // If no message headers but binding requires headers, no match
        const headers = message_headers orelse return false;

        // Parse binding arguments as JSON to extract header match criteria
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const temp_allocator = arena.allocator();

        const parsed = std.json.parseFromSlice(std.json.Value, temp_allocator, args, .{}) catch {
            return false; // Invalid JSON format
        };
        defer parsed.deinit();

        const match_criteria = parsed.value.object;

        // Get match type: "all" (default) or "any"
        var match_all = true;
        if (match_criteria.get("x-match")) |x_match_value| {
            if (x_match_value == .string) {
                if (std.mem.eql(u8, x_match_value.string, "any")) {
                    match_all = false;
                }
            }
        }

        var matched_count: u32 = 0;
        var required_count: u32 = 0;

        // Check each header requirement
        var criteria_iter = match_criteria.iterator();
        while (criteria_iter.next()) |entry| {
            const key = entry.key_ptr.*;
            const expected_value = entry.value_ptr.*;

            // Skip x-match meta parameter
            if (std.mem.eql(u8, key, "x-match")) continue;

            required_count += 1;

            // Check if message has this header
            if (headers.get(key)) |actual_value| {
                // Compare values based on type
                const values_match = switch (expected_value) {
                    .string => |str| std.mem.eql(u8, actual_value, str),
                    .integer => |int| blk: {
                        const str_int = std.fmt.parseInt(i64, actual_value, 10) catch break :blk false;
                        break :blk str_int == int;
                    },
                    .float => |float| blk: {
                        const str_float = std.fmt.parseFloat(f64, actual_value) catch break :blk false;
                        break :blk str_float == float;
                    },
                    .bool => |boolean| blk: {
                        if (std.mem.eql(u8, actual_value, "true")) {
                            break :blk boolean;
                        } else if (std.mem.eql(u8, actual_value, "false")) {
                            break :blk !boolean;
                        }
                        break :blk false;
                    },
                    else => false,
                };

                if (values_match) {
                    matched_count += 1;
                    if (!match_all) {
                        return true; // For "any" match, one match is enough
                    }
                }
            }
        }

        // For "all" match, all headers must match
        if (match_all) {
            return matched_count == required_count and required_count > 0;
        }

        // For "any" match, at least one header must match
        return matched_count > 0;
    }

    pub fn getBindingCount(self: *const Exchange) u32 {
        return @intCast(self.bindings.items.len);
    }

    pub fn getStats(self: *const Exchange, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("name", std.json.Value{ .string = self.name });
        try stats.put("type", std.json.Value{ .string = @tagName(self.exchange_type) });
        try stats.put("durable", std.json.Value{ .bool = self.durable });
        try stats.put("auto_delete", std.json.Value{ .bool = self.auto_delete });
        try stats.put("internal", std.json.Value{ .bool = self.internal });
        try stats.put("bindings", std.json.Value{ .integer = @intCast(self.bindings.items.len) });
        try stats.put("messages_in", std.json.Value{ .integer = @intCast(self.messages_in) });
        try stats.put("messages_out", std.json.Value{ .integer = @intCast(self.messages_out) });
        try stats.put("bytes_in", std.json.Value{ .integer = @intCast(self.bytes_in) });
        try stats.put("bytes_out", std.json.Value{ .integer = @intCast(self.bytes_out) });

        return std.json.Value{ .object = stats };
    }
};

// Topic pattern matching using std.mem operations (no regex dependency)
fn matchTopicWildcards(pattern: []const u8, routing_key: []const u8) bool {
    var pattern_parts = std.mem.splitScalar(u8, pattern, '.');
    var key_parts = std.mem.splitScalar(u8, routing_key, '.');

    var pattern_list = std.ArrayList([]const u8).init(std.heap.page_allocator);
    defer pattern_list.deinit();

    var key_list = std.ArrayList([]const u8).init(std.heap.page_allocator);
    defer key_list.deinit();

    // Collect pattern parts
    while (pattern_parts.next()) |part| {
        pattern_list.append(part) catch return false;
    }

    // Collect key parts
    while (key_parts.next()) |part| {
        key_list.append(part) catch return false;
    }

    return matchTopicParts(pattern_list.items, key_list.items);
}

fn matchTopicParts(pattern_parts: [][]const u8, key_parts: [][]const u8) bool {
    var p_idx: usize = 0;
    var k_idx: usize = 0;

    while (p_idx < pattern_parts.len and k_idx < key_parts.len) {
        const pattern_part = pattern_parts[p_idx];
        const key_part = key_parts[k_idx];

        if (std.mem.eql(u8, pattern_part, "#")) {
            // # matches zero or more words
            if (p_idx == pattern_parts.len - 1) {
                // # is the last pattern part, matches everything remaining
                return true;
            }

            // Try to match the rest of the pattern with remaining key parts
            for (k_idx..key_parts.len + 1) |next_k_idx| {
                if (matchTopicParts(pattern_parts[p_idx + 1 ..], key_parts[next_k_idx..])) {
                    return true;
                }
            }
            return false;
        } else if (std.mem.eql(u8, pattern_part, "*")) {
            // * matches exactly one word
            p_idx += 1;
            k_idx += 1;
        } else {
            // Exact match required
            if (!std.mem.eql(u8, pattern_part, key_part)) {
                return false;
            }
            p_idx += 1;
            k_idx += 1;
        }
    }

    // Check if we've consumed all parts correctly
    if (p_idx == pattern_parts.len and k_idx == key_parts.len) {
        return true;
    }

    // Handle trailing # in pattern
    if (p_idx < pattern_parts.len and
        p_idx == pattern_parts.len - 1 and
        std.mem.eql(u8, pattern_parts[p_idx], "#"))
    {
        return true;
    }

    return false;
}

test "exchange creation and binding" {
    const allocator = std.testing.allocator;

    var exchange = try Exchange.init(allocator, "test.exchange", .direct, true, false, false, null);
    defer exchange.deinit();

    try std.testing.expectEqualStrings("test.exchange", exchange.name);
    try std.testing.expectEqual(ExchangeType.direct, exchange.exchange_type);
    try std.testing.expectEqual(true, exchange.durable);
    try std.testing.expectEqual(@as(u32, 0), exchange.getBindingCount());

    // Add a binding
    try exchange.bindQueue("test.queue", "test.key", null);
    try std.testing.expectEqual(@as(u32, 1), exchange.getBindingCount());

    // Remove the binding
    try exchange.unbindQueue("test.queue", "test.key", null);
    try std.testing.expectEqual(@as(u32, 0), exchange.getBindingCount());
}

test "topic pattern matching" {
    try std.testing.expect(matchTopicWildcards("*", "word"));
    try std.testing.expect(matchTopicWildcards("*.stock.*", "usd.stock.db"));
    try std.testing.expect(matchTopicWildcards("stock.#", "stock.usd.nyse"));
    try std.testing.expect(matchTopicWildcards("stock.#", "stock"));
    try std.testing.expect(matchTopicWildcards("#", "anything.goes.here"));
    try std.testing.expect(matchTopicWildcards("#.last", "some.thing.last"));

    try std.testing.expect(!matchTopicWildcards("*", "two.words"));
    try std.testing.expect(!matchTopicWildcards("*.stock.*", "usd.nyse"));
    try std.testing.expect(!matchTopicWildcards("stock.usd", "stock.eur"));
}

test "exchange routing" {
    const allocator = std.testing.allocator;

    // Test direct exchange
    var direct_exchange = try Exchange.init(allocator, "direct", .direct, true, false, false, null);
    defer direct_exchange.deinit();

    try direct_exchange.bindQueue("queue1", "key1", null);
    try direct_exchange.bindQueue("queue2", "key2", null);

    var message = try Message.init(allocator, 1, "direct", "key1", "test message");
    defer message.deinit();

    const matched_queues = try direct_exchange.routeMessage(&message, allocator);
    defer allocator.free(matched_queues);

    try std.testing.expectEqual(@as(usize, 1), matched_queues.len);
    try std.testing.expectEqualStrings("queue1", matched_queues[0]);

    // Test fanout exchange
    var fanout_exchange = try Exchange.init(allocator, "fanout", .fanout, true, false, false, null);
    defer fanout_exchange.deinit();

    try fanout_exchange.bindQueue("queue1", "", null);
    try fanout_exchange.bindQueue("queue2", "", null);

    const fanout_matched = try fanout_exchange.routeMessage(&message, allocator);
    defer allocator.free(fanout_matched);

    try std.testing.expectEqual(@as(usize, 2), fanout_matched.len);
}

test "exchange binding arguments matching" {
    const allocator = std.testing.allocator;

    var exchange = try Exchange.init(allocator, "test", .direct, true, false, false, null);
    defer exchange.deinit();

    // Add bindings with different arguments
    try exchange.bindQueue("queue1", "key1", null);
    try exchange.bindQueue("queue2", "key1", "args1");
    try exchange.bindQueue("queue3", "key1", "args2");

    try std.testing.expectEqual(@as(u32, 3), exchange.getBindingCount());

    // Unbind with null arguments should only remove the null binding
    try exchange.unbindQueue("queue1", "key1", null);
    try std.testing.expectEqual(@as(u32, 2), exchange.getBindingCount());

    // Unbind with specific arguments should only remove matching binding
    try exchange.unbindQueue("queue2", "key1", "args1");
    try std.testing.expectEqual(@as(u32, 1), exchange.getBindingCount());

    // Trying to unbind with wrong arguments should fail
    const result = exchange.unbindQueue("queue3", "key1", "wrong_args");
    try std.testing.expectError(error.BindingNotFound, result);
    try std.testing.expectEqual(@as(u32, 1), exchange.getBindingCount());

    // Unbind with correct arguments should succeed
    try exchange.unbindQueue("queue3", "key1", "args2");
    try std.testing.expectEqual(@as(u32, 0), exchange.getBindingCount());
}

test "headers exchange matching" {
    const allocator = std.testing.allocator;

    var headers_exchange = try Exchange.init(allocator, "test", .headers, true, false, false, null);
    defer headers_exchange.deinit();

    // Create test message with headers
    var message = try Message.init(allocator, 1, "test", "test.key", "test body");
    defer message.deinit();

    // Set up message headers
    message.headers = HeaderTable.init(allocator);
    try message.headers.?.put("type", "order");
    try message.headers.?.put("priority", "high");
    try message.headers.?.put("region", "us-east");
    defer if (message.headers) |*headers| headers.deinit();

    // Test "all" matching (default)
    const all_args = "{\"x-match\":\"all\",\"type\":\"order\",\"priority\":\"high\"}";
    try headers_exchange.bindQueue("queue1", "", all_args);

    var matched = try headers_exchange.routeMessage(&message, allocator);
    defer allocator.free(matched);
    try std.testing.expectEqual(@as(usize, 1), matched.len);

    // Test "any" matching
    const any_args = "{\"x-match\":\"any\",\"type\":\"order\",\"status\":\"pending\"}";
    try headers_exchange.bindQueue("queue2", "", any_args);

    matched = try headers_exchange.routeMessage(&message, allocator);
    defer allocator.free(matched);
    try std.testing.expectEqual(@as(usize, 2), matched.len);

    // Test no match (missing required header)
    const no_match_args = "{\"x-match\":\"all\",\"type\":\"order\",\"missing\":\"value\"}";
    try headers_exchange.bindQueue("queue3", "", no_match_args);

    matched = try headers_exchange.routeMessage(&message, allocator);
    defer allocator.free(matched);
    try std.testing.expectEqual(@as(usize, 2), matched.len); // Still just queue1 and queue2

    // Test null binding args (matches everything)
    try headers_exchange.bindQueue("queue4", "", null);

    matched = try headers_exchange.routeMessage(&message, allocator);
    defer allocator.free(matched);
    try std.testing.expectEqual(@as(usize, 3), matched.len);
}

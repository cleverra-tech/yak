const std = @import("std");

pub const Binding = struct {
    queue_name: []const u8,
    exchange_name: []const u8,
    routing_key: []const u8,
    arguments: ?[]const u8,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        queue_name: []const u8,
        exchange_name: []const u8,
        routing_key: []const u8,
        arguments: ?[]const u8,
    ) !Binding {
        return Binding{
            .queue_name = try allocator.dupe(u8, queue_name),
            .exchange_name = try allocator.dupe(u8, exchange_name),
            .routing_key = try allocator.dupe(u8, routing_key),
            .arguments = if (arguments) |args| try allocator.dupe(u8, args) else null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Binding) void {
        self.allocator.free(self.queue_name);
        self.allocator.free(self.exchange_name);
        self.allocator.free(self.routing_key);
        if (self.arguments) |args| {
            self.allocator.free(args);
        }
    }

    pub fn encodeForStorage(self: *const Binding, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try writeString(buffer.writer(), self.queue_name);
        try writeString(buffer.writer(), self.exchange_name);
        try writeString(buffer.writer(), self.routing_key);

        if (self.arguments) |args| {
            try buffer.writer().writeInt(u8, 1, .little);
            try writeString(buffer.writer(), args);
        } else {
            try buffer.writer().writeInt(u8, 0, .little);
        }

        return buffer.toOwnedSlice();
    }

    pub fn decodeFromStorage(data: []const u8, allocator: std.mem.Allocator) !Binding {
        var reader = std.io.fixedBufferStream(data).reader();

        const queue_name = try readString(reader, allocator);
        const exchange_name = try readString(reader, allocator);
        const routing_key = try readString(reader, allocator);

        const has_args = (try reader.readInt(u8, .little)) != 0;
        const arguments = if (has_args) try readString(reader, allocator) else null;

        return Binding{
            .queue_name = queue_name,
            .exchange_name = exchange_name,
            .routing_key = routing_key,
            .arguments = arguments,
            .allocator = allocator,
        };
    }

    pub fn matches(self: *const Binding, other: *const Binding) bool {
        return std.mem.eql(u8, self.queue_name, other.queue_name) and
            std.mem.eql(u8, self.exchange_name, other.exchange_name) and
            std.mem.eql(u8, self.routing_key, other.routing_key) and
            self.argumentsMatch(other.arguments);
    }

    pub fn argumentsMatch(self: *const Binding, other_args: ?[]const u8) bool {
        if (self.arguments == null and other_args == null) return true;
        if (self.arguments == null or other_args == null) return false;
        return std.mem.eql(u8, self.arguments.?, other_args.?);
    }

    pub fn getStorageKey(self: *const Binding, allocator: std.mem.Allocator) ![]u8 {
        return try std.fmt.allocPrint(allocator, "binding:{s}:{s}:{s}", .{ self.exchange_name, self.queue_name, self.routing_key });
    }

    pub fn clone(self: *const Binding, allocator: std.mem.Allocator) !Binding {
        return Binding.init(
            allocator,
            self.queue_name,
            self.exchange_name,
            self.routing_key,
            self.arguments,
        );
    }

    pub fn getStats(self: *const Binding, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);

        try stats.put("queue_name", std.json.Value{ .string = self.queue_name });
        try stats.put("exchange_name", std.json.Value{ .string = self.exchange_name });
        try stats.put("routing_key", std.json.Value{ .string = self.routing_key });

        if (self.arguments) |args| {
            try stats.put("arguments", std.json.Value{ .string = args });
        } else {
            try stats.put("arguments", std.json.Value{ .null = {} });
        }

        return std.json.Value{ .object = stats };
    }
};

// Helper functions for serialization
fn writeString(writer: anytype, str: []const u8) !void {
    try writer.writeInt(u32, @intCast(str.len), .little);
    try writer.writeAll(str);
}

fn readString(reader: anytype, allocator: std.mem.Allocator) ![]u8 {
    const len = try reader.readInt(u32, .little);
    const str = try allocator.alloc(u8, len);
    try reader.readNoEof(str);
    return str;
}

// Binding collection for efficient management
pub const BindingSet = struct {
    bindings: std.ArrayList(Binding),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) BindingSet {
        return BindingSet{
            .bindings = std.ArrayList(Binding).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BindingSet) void {
        for (self.bindings.items) |*binding| {
            binding.deinit();
        }
        self.bindings.deinit();
    }

    pub fn add(self: *BindingSet, binding: Binding) !void {
        // Check for duplicates
        for (self.bindings.items) |existing| {
            if (existing.matches(&binding)) {
                return; // Binding already exists
            }
        }
        try self.bindings.append(binding);
    }

    pub fn remove(self: *BindingSet, binding: *const Binding) !void {
        for (self.bindings.items, 0..) |existing, i| {
            if (existing.matches(binding)) {
                var removed = self.bindings.swapRemove(i);
                removed.deinit();
                return;
            }
        }
        return error.BindingNotFound;
    }

    pub fn findByExchange(self: *const BindingSet, exchange_name: []const u8, allocator: std.mem.Allocator) ![]Binding {
        var result = std.ArrayList(Binding).init(allocator);
        defer result.deinit();

        for (self.bindings.items) |binding| {
            if (std.mem.eql(u8, binding.exchange_name, exchange_name)) {
                try result.append(try binding.clone(allocator));
            }
        }

        return result.toOwnedSlice();
    }

    pub fn findByQueue(self: *const BindingSet, queue_name: []const u8, allocator: std.mem.Allocator) ![]Binding {
        var result = std.ArrayList(Binding).init(allocator);
        defer result.deinit();

        for (self.bindings.items) |binding| {
            if (std.mem.eql(u8, binding.queue_name, queue_name)) {
                try result.append(try binding.clone(allocator));
            }
        }

        return result.toOwnedSlice();
    }

    pub fn count(self: *const BindingSet) u32 {
        return @intCast(self.bindings.items.len);
    }

    pub fn removeAllForExchange(self: *BindingSet, exchange_name: []const u8) u32 {
        var removed_count: u32 = 0;
        var i: usize = 0;

        while (i < self.bindings.items.len) {
            if (std.mem.eql(u8, self.bindings.items[i].exchange_name, exchange_name)) {
                var removed = self.bindings.swapRemove(i);
                removed.deinit();
                removed_count += 1;
            } else {
                i += 1;
            }
        }

        return removed_count;
    }

    pub fn removeAllForQueue(self: *BindingSet, queue_name: []const u8) u32 {
        var removed_count: u32 = 0;
        var i: usize = 0;

        while (i < self.bindings.items.len) {
            if (std.mem.eql(u8, self.bindings.items[i].queue_name, queue_name)) {
                var removed = self.bindings.swapRemove(i);
                removed.deinit();
                removed_count += 1;
            } else {
                i += 1;
            }
        }

        return removed_count;
    }
};

test "binding creation and serialization" {
    const allocator = std.testing.allocator;

    var binding = try Binding.init(allocator, "test.queue", "test.exchange", "test.key", "test args");
    defer binding.deinit();

    try std.testing.expectEqualStrings("test.queue", binding.queue_name);
    try std.testing.expectEqualStrings("test.exchange", binding.exchange_name);
    try std.testing.expectEqualStrings("test.key", binding.routing_key);
    try std.testing.expectEqualStrings("test args", binding.arguments.?);

    // Test serialization
    const encoded = try binding.encodeForStorage(allocator);
    defer allocator.free(encoded);

    var decoded = try Binding.decodeFromStorage(encoded, allocator);
    defer decoded.deinit();

    try std.testing.expectEqualStrings(binding.queue_name, decoded.queue_name);
    try std.testing.expectEqualStrings(binding.exchange_name, decoded.exchange_name);
    try std.testing.expectEqualStrings(binding.routing_key, decoded.routing_key);
    try std.testing.expectEqualStrings(binding.arguments.?, decoded.arguments.?);
}

test "binding set operations" {
    const allocator = std.testing.allocator;

    var binding_set = BindingSet.init(allocator);
    defer binding_set.deinit();

    try std.testing.expectEqual(@as(u32, 0), binding_set.count());

    // Add bindings
    const binding1 = try Binding.init(allocator, "queue1", "exchange1", "key1", null);
    const binding2 = try Binding.init(allocator, "queue2", "exchange1", "key2", null);
    const binding3 = try Binding.init(allocator, "queue1", "exchange2", "key1", null);

    try binding_set.add(binding1);
    try binding_set.add(binding2);
    try binding_set.add(binding3);

    try std.testing.expectEqual(@as(u32, 3), binding_set.count());

    // Find by exchange
    const exchange1_bindings = try binding_set.findByExchange("exchange1", allocator);
    defer {
        for (exchange1_bindings) |*binding| {
            binding.deinit();
        }
        allocator.free(exchange1_bindings);
    }
    try std.testing.expectEqual(@as(usize, 2), exchange1_bindings.len);

    // Remove all for exchange
    const removed_count = binding_set.removeAllForExchange("exchange1");
    try std.testing.expectEqual(@as(u32, 2), removed_count);
    try std.testing.expectEqual(@as(u32, 1), binding_set.count());
}

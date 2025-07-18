const std = @import("std");

pub const HeaderTable = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage);

pub const Message = struct {
    id: u64,
    exchange: []const u8,
    routing_key: []const u8,
    body: []const u8,
    headers: ?HeaderTable,
    persistent: bool,
    delivery_count: u32,
    timestamp: i64,

    // Wombat storage metadata
    wombat_key: []const u8,
    // value_pointer: ?wombat.ValuePointer, // Commented out until Wombat is available

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, id: u64, exchange: []const u8, routing_key: []const u8, body: []const u8) !Message {
        const timestamp = std.time.timestamp();

        return Message{
            .id = id,
            .exchange = try allocator.dupe(u8, exchange),
            .routing_key = try allocator.dupe(u8, routing_key),
            .body = try allocator.dupe(u8, body),
            .headers = null,
            .persistent = false,
            .delivery_count = 0,
            .timestamp = timestamp,
            .wombat_key = &[_]u8{},
            // .value_pointer = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Message) void {
        self.allocator.free(self.exchange);
        self.allocator.free(self.routing_key);
        self.allocator.free(self.body);

        if (self.headers) |*headers| {
            var iterator = headers.iterator();
            while (iterator.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            headers.deinit();
        }

        if (self.wombat_key.len > 0) {
            self.allocator.free(self.wombat_key);
        }
    }

    pub fn setHeader(self: *Message, key: []const u8, value: []const u8) !void {
        if (self.headers == null) {
            self.headers = HeaderTable.init(self.allocator);
        }

        const owned_key = try self.allocator.dupe(u8, key);
        const owned_value = try self.allocator.dupe(u8, value);

        try self.headers.?.put(owned_key, owned_value);
    }

    pub fn getHeader(self: *const Message, key: []const u8) ?[]const u8 {
        if (self.headers) |headers| {
            return headers.get(key);
        }
        return null;
    }

    pub fn encodeForStorage(self: *const Message, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.writer().writeInt(u64, self.id, .little);
        try buffer.writer().writeInt(u64, @intCast(self.timestamp), .little);
        try buffer.writer().writeInt(u32, self.delivery_count, .little);
        try buffer.writer().writeInt(u8, if (self.persistent) 1 else 0, .little);

        // Write variable-length fields
        try writeString(buffer.writer(), self.exchange);
        try writeString(buffer.writer(), self.routing_key);
        try writeString(buffer.writer(), self.body);

        // Write headers if present
        if (self.headers) |headers| {
            try buffer.writer().writeInt(u8, 1, .little); // has_headers flag
            try writeHeaders(buffer.writer(), headers);
        } else {
            try buffer.writer().writeInt(u8, 0, .little); // no headers
        }

        return buffer.toOwnedSlice();
    }

    pub fn decodeFromStorage(data: []const u8, allocator: std.mem.Allocator) !Message {
        var stream = std.io.fixedBufferStream(data);
        var reader = stream.reader();

        const id = try reader.readInt(u64, .little);
        const timestamp = try reader.readInt(u64, .little);
        const delivery_count = try reader.readInt(u32, .little);
        const persistent = (try reader.readInt(u8, .little)) != 0;

        const exchange = try readString(reader, allocator);
        const routing_key = try readString(reader, allocator);
        const body = try readString(reader, allocator);

        // Read headers if present
        const has_headers = (try reader.readInt(u8, .little)) != 0;
        const headers = if (has_headers) try readHeaders(reader, allocator) else null;

        return Message{
            .id = id,
            .exchange = exchange,
            .routing_key = routing_key,
            .body = body,
            .headers = headers,
            .persistent = persistent,
            .delivery_count = delivery_count,
            .timestamp = @intCast(timestamp),
            .wombat_key = &[_]u8{},
            // .value_pointer = null,
            .allocator = allocator,
        };
    }

    pub fn incrementDeliveryCount(self: *Message) void {
        self.delivery_count += 1;
    }

    pub fn markPersistent(self: *Message) void {
        self.persistent = true;
    }

    pub fn clone(self: *const Message, allocator: std.mem.Allocator) !Message {
        var cloned = try Message.init(allocator, self.id, self.exchange, self.routing_key, self.body);
        cloned.persistent = self.persistent;
        cloned.delivery_count = self.delivery_count;
        cloned.timestamp = self.timestamp;

        if (self.headers) |headers| {
            cloned.headers = HeaderTable.init(allocator);
            var iterator = headers.iterator();
            while (iterator.next()) |entry| {
                try cloned.setHeader(entry.key_ptr.*, entry.value_ptr.*);
            }
        }

        return cloned;
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

fn writeHeaders(writer: anytype, headers: HeaderTable) !void {
    try writer.writeInt(u32, @intCast(headers.count()), .little);
    var iterator = headers.iterator();
    while (iterator.next()) |entry| {
        try writeString(writer, entry.key_ptr.*);
        try writeString(writer, entry.value_ptr.*);
    }
}

fn readHeaders(reader: anytype, allocator: std.mem.Allocator) !HeaderTable {
    const count = try reader.readInt(u32, .little);
    var headers = HeaderTable.init(allocator);

    for (0..count) |_| {
        const key = try readString(reader, allocator);
        const value = try readString(reader, allocator);
        try headers.put(key, value);
    }

    return headers;
}

test "message creation and manipulation" {
    const allocator = std.testing.allocator;

    var message = try Message.init(allocator, 1, "test.exchange", "test.key", "Hello, World!");
    defer message.deinit();

    try std.testing.expectEqual(@as(u64, 1), message.id);
    try std.testing.expectEqualStrings("test.exchange", message.exchange);
    try std.testing.expectEqualStrings("test.key", message.routing_key);
    try std.testing.expectEqualStrings("Hello, World!", message.body);
    try std.testing.expectEqual(@as(u32, 0), message.delivery_count);
    try std.testing.expectEqual(false, message.persistent);

    message.incrementDeliveryCount();
    try std.testing.expectEqual(@as(u32, 1), message.delivery_count);

    message.markPersistent();
    try std.testing.expectEqual(true, message.persistent);
}

test "message headers" {
    const allocator = std.testing.allocator;

    var message = try Message.init(allocator, 1, "test.exchange", "test.key", "Hello, World!");
    defer message.deinit();

    try message.setHeader("content-type", "text/plain");
    try message.setHeader("user-id", "test-user");

    try std.testing.expectEqualStrings("text/plain", message.getHeader("content-type").?);
    try std.testing.expectEqualStrings("test-user", message.getHeader("user-id").?);
    try std.testing.expectEqual(@as(?[]const u8, null), message.getHeader("non-existent"));
}

test "message serialization" {
    const allocator = std.testing.allocator;

    var original = try Message.init(allocator, 42, "test.exchange", "test.key", "Test message body");
    defer original.deinit();

    try original.setHeader("content-type", "application/json");
    original.markPersistent();
    original.incrementDeliveryCount();

    const encoded = try original.encodeForStorage(allocator);
    defer allocator.free(encoded);

    var decoded = try Message.decodeFromStorage(encoded, allocator);
    defer decoded.deinit();

    try std.testing.expectEqual(original.id, decoded.id);
    try std.testing.expectEqualStrings(original.exchange, decoded.exchange);
    try std.testing.expectEqualStrings(original.routing_key, decoded.routing_key);
    try std.testing.expectEqualStrings(original.body, decoded.body);
    try std.testing.expectEqual(original.persistent, decoded.persistent);
    try std.testing.expectEqual(original.delivery_count, decoded.delivery_count);
    try std.testing.expectEqualStrings("application/json", decoded.getHeader("content-type").?);
}

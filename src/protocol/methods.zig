const std = @import("std");

// AMQP Class IDs
pub const ClassId = enum(u16) {
    connection = 10,
    channel = 20,
    exchange = 40,
    queue = 50,
    basic = 60,
    tx = 90,
    confirm = 85,
};

// Connection Methods
pub const ConnectionMethod = enum(u16) {
    start = 10,
    start_ok = 11,
    secure = 20,
    secure_ok = 21,
    tune = 30,
    tune_ok = 31,
    open = 40,
    open_ok = 41,
    close = 50,
    close_ok = 51,
    blocked = 60,
    unblocked = 61,
};

// Channel Methods
pub const ChannelMethod = enum(u16) {
    open = 10,
    open_ok = 11,
    flow = 20,
    flow_ok = 21,
    close = 40,
    close_ok = 41,
};

// Exchange Methods
pub const ExchangeMethod = enum(u16) {
    declare = 10,
    declare_ok = 11,
    delete = 20,
    delete_ok = 21,
    bind = 30,
    bind_ok = 31,
    unbind = 40,
    unbind_ok = 51,
};

// Queue Methods
pub const QueueMethod = enum(u16) {
    declare = 10,
    declare_ok = 11,
    bind = 20,
    bind_ok = 21,
    purge = 30,
    purge_ok = 31,
    delete = 40,
    delete_ok = 41,
    unbind = 50,
    unbind_ok = 51,
};

// Basic Methods
pub const BasicMethod = enum(u16) {
    qos = 10,
    qos_ok = 11,
    consume = 20,
    consume_ok = 21,
    cancel = 30,
    cancel_ok = 31,
    publish = 40,
    @"return" = 50,
    deliver = 60,
    get = 70,
    get_ok = 71,
    get_empty = 72,
    ack = 80,
    reject = 90,
    recover_async = 100,
    recover = 110,
    recover_ok = 111,
    nack = 120,
};

// Transaction Methods
pub const TxMethod = enum(u16) {
    select = 10,
    select_ok = 11,
    commit = 20,
    commit_ok = 21,
    rollback = 30,
    rollback_ok = 31,
};

// Confirm Methods
pub const ConfirmMethod = enum(u16) {
    select = 10,
    select_ok = 11,
};

// Method Frame Structure
pub const MethodFrame = struct {
    class_id: u16,
    method_id: u16,
    arguments: []const u8,

    pub fn encode(self: *const MethodFrame, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.writer().writeInt(u16, self.class_id, .big);
        try buffer.writer().writeInt(u16, self.method_id, .big);
        try buffer.appendSlice(self.arguments);

        return buffer.toOwnedSlice();
    }

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !MethodFrame {
        if (data.len < 4) return error.InvalidMethodFrame;

        const class_id = std.mem.readInt(u16, data[0..2], .big);
        const method_id = std.mem.readInt(u16, data[2..4], .big);
        const arguments = try allocator.dupe(u8, data[4..]);

        return MethodFrame{
            .class_id = class_id,
            .method_id = method_id,
            .arguments = arguments,
        };
    }

    pub fn deinit(self: *const MethodFrame, allocator: std.mem.Allocator) void {
        allocator.free(self.arguments);
    }
};

// Specific method structures
pub const ConnectionStart = struct {
    version_major: u8,
    version_minor: u8,
    server_properties: []const u8, // Field table
    mechanisms: []const u8,
    locales: []const u8,

    pub fn encode(self: *const ConnectionStart, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.writer().writeInt(u8, self.version_major, .big);
        try buffer.writer().writeInt(u8, self.version_minor, .big);
        try writeShortString(buffer.writer(), self.server_properties);
        try writeLongString(buffer.writer(), self.mechanisms);
        try writeLongString(buffer.writer(), self.locales);

        return buffer.toOwnedSlice();
    }
};

pub const ConnectionStartOk = struct {
    client_properties: []const u8, // Field table
    mechanism: []const u8,
    response: []const u8,
    locale: []const u8,

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !ConnectionStartOk {
        const reader = std.io.fixedBufferStream(data).reader();

        const client_properties = try readShortString(reader, allocator);
        const mechanism = try readShortString(reader, allocator);
        const response = try readLongString(reader, allocator);
        const locale = try readShortString(reader, allocator);

        return ConnectionStartOk{
            .client_properties = client_properties,
            .mechanism = mechanism,
            .response = response,
            .locale = locale,
        };
    }
};

pub const ConnectionTune = struct {
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,

    pub fn encode(self: *const ConnectionTune, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.writer().writeInt(u16, self.channel_max, .big);
        try buffer.writer().writeInt(u32, self.frame_max, .big);
        try buffer.writer().writeInt(u16, self.heartbeat, .big);

        return buffer.toOwnedSlice();
    }
};

pub const ConnectionTuneOk = struct {
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,

    pub fn decode(data: []const u8) !ConnectionTuneOk {
        if (data.len < 8) return error.InvalidTuneOk;

        return ConnectionTuneOk{
            .channel_max = std.mem.readInt(u16, data[0..2], .big),
            .frame_max = std.mem.readInt(u32, data[2..6], .big),
            .heartbeat = std.mem.readInt(u16, data[6..8], .big),
        };
    }
};

pub const ConnectionOpen = struct {
    virtual_host: []const u8,
    capabilities: []const u8,
    insist: bool,

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !ConnectionOpen {
        const reader = std.io.fixedBufferStream(data).reader();

        const virtual_host = try readShortString(reader, allocator);
        const capabilities = try readShortString(reader, allocator);
        const insist_byte = try reader.readByte();

        return ConnectionOpen{
            .virtual_host = virtual_host,
            .capabilities = capabilities,
            .insist = (insist_byte & 0x01) != 0,
        };
    }
};

pub const BasicPublish = struct {
    exchange: []const u8,
    routing_key: []const u8,
    mandatory: bool,
    immediate: bool,

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !BasicPublish {
        const reader = std.io.fixedBufferStream(data).reader();

        _ = try reader.readInt(u16, .big); // reserved
        const exchange = try readShortString(reader, allocator);
        const routing_key = try readShortString(reader, allocator);
        const flags = try reader.readByte();

        return BasicPublish{
            .exchange = exchange,
            .routing_key = routing_key,
            .mandatory = (flags & 0x01) != 0,
            .immediate = (flags & 0x02) != 0,
        };
    }
};

pub const BasicConsume = struct {
    queue: []const u8,
    consumer_tag: []const u8,
    no_local: bool,
    no_ack: bool,
    exclusive: bool,
    no_wait: bool,
    arguments: []const u8, // Field table

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !BasicConsume {
        const reader = std.io.fixedBufferStream(data).reader();

        _ = try reader.readInt(u16, .big); // reserved
        const queue = try readShortString(reader, allocator);
        const consumer_tag = try readShortString(reader, allocator);
        const flags = try reader.readByte();
        const arguments = try readFieldTable(reader, allocator);

        return BasicConsume{
            .queue = queue,
            .consumer_tag = consumer_tag,
            .no_local = (flags & 0x01) != 0,
            .no_ack = (flags & 0x02) != 0,
            .exclusive = (flags & 0x04) != 0,
            .no_wait = (flags & 0x08) != 0,
            .arguments = arguments,
        };
    }
};

pub const BasicAck = struct {
    delivery_tag: u64,
    multiple: bool,

    pub fn decode(data: []const u8) !BasicAck {
        if (data.len < 9) return error.InvalidAck;

        return BasicAck{
            .delivery_tag = std.mem.readInt(u64, data[0..8], .big),
            .multiple = (data[8] & 0x01) != 0,
        };
    }

    pub fn encode(self: *const BasicAck, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.writer().writeInt(u64, self.delivery_tag, .big);
        try buffer.writer().writeInt(u8, if (self.multiple) 1 else 0, .big);

        return buffer.toOwnedSlice();
    }
};

// Helper functions for AMQP encoding/decoding
fn writeShortString(writer: anytype, str: []const u8) !void {
    try writer.writeInt(u8, @intCast(str.len), .big);
    try writer.writeAll(str);
}

fn readShortString(reader: anytype, allocator: std.mem.Allocator) ![]u8 {
    const len = try reader.readInt(u8, .big);
    const str = try allocator.alloc(u8, len);
    try reader.readNoEof(str);
    return str;
}

fn writeLongString(writer: anytype, str: []const u8) !void {
    try writer.writeInt(u32, @intCast(str.len), .big);
    try writer.writeAll(str);
}

fn readLongString(reader: anytype, allocator: std.mem.Allocator) ![]u8 {
    const len = try reader.readInt(u32, .big);
    const str = try allocator.alloc(u8, len);
    try reader.readNoEof(str);
    return str;
}

fn readFieldTable(reader: anytype, allocator: std.mem.Allocator) ![]u8 {
    const len = try reader.readInt(u32, .big);
    const table = try allocator.alloc(u8, len);
    try reader.readNoEof(table);
    return table;
}

test "method frame encoding and decoding" {
    const allocator = std.testing.allocator;

    const args = "test arguments";
    const original = MethodFrame{
        .class_id = @intFromEnum(ClassId.connection),
        .method_id = @intFromEnum(ConnectionMethod.start),
        .arguments = args,
    };

    const encoded = try original.encode(allocator);
    defer allocator.free(encoded);

    var decoded = try MethodFrame.decode(encoded, allocator);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(original.class_id, decoded.class_id);
    try std.testing.expectEqual(original.method_id, decoded.method_id);
    try std.testing.expectEqualStrings(original.arguments, decoded.arguments);
}

test "basic ack encoding and decoding" {
    const allocator = std.testing.allocator;

    const original = BasicAck{
        .delivery_tag = 12345,
        .multiple = true,
    };

    const encoded = try original.encode(allocator);
    defer allocator.free(encoded);

    const decoded = try BasicAck.decode(encoded);

    try std.testing.expectEqual(original.delivery_tag, decoded.delivery_tag);
    try std.testing.expectEqual(original.multiple, decoded.multiple);
}

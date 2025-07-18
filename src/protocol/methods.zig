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

// AMQP Content Header (Basic Properties)
pub const BasicProperties = struct {
    content_type: ?[]const u8,
    content_encoding: ?[]const u8,
    headers: ?[]const u8, // Field table
    delivery_mode: ?u8,
    priority: ?u8,
    correlation_id: ?[]const u8,
    reply_to: ?[]const u8,
    expiration: ?[]const u8,
    message_id: ?[]const u8,
    timestamp: ?u64,
    type: ?[]const u8,
    user_id: ?[]const u8,
    app_id: ?[]const u8,
    cluster_id: ?[]const u8,

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !BasicProperties {
        if (data.len < 2) return error.InvalidBasicProperties;

        var stream = std.io.fixedBufferStream(data);
        const reader = stream.reader();

        // Read property flags (2 bytes)
        const property_flags = try reader.readInt(u16, .big);

        var properties = BasicProperties{
            .content_type = null,
            .content_encoding = null,
            .headers = null,
            .delivery_mode = null,
            .priority = null,
            .correlation_id = null,
            .reply_to = null,
            .expiration = null,
            .message_id = null,
            .timestamp = null,
            .type = null,
            .user_id = null,
            .app_id = null,
            .cluster_id = null,
        };

        // Parse properties based on flags
        if (property_flags & 0x8000 != 0) { // content-type
            properties.content_type = try readShortString(reader, allocator);
        }
        if (property_flags & 0x4000 != 0) { // content-encoding
            properties.content_encoding = try readShortString(reader, allocator);
        }
        if (property_flags & 0x2000 != 0) { // headers (field table)
            properties.headers = try readFieldTable(reader, allocator);
        }
        if (property_flags & 0x1000 != 0) { // delivery-mode
            properties.delivery_mode = try reader.readByte();
        }
        if (property_flags & 0x0800 != 0) { // priority
            properties.priority = try reader.readByte();
        }
        if (property_flags & 0x0400 != 0) { // correlation-id
            properties.correlation_id = try readShortString(reader, allocator);
        }
        if (property_flags & 0x0200 != 0) { // reply-to
            properties.reply_to = try readShortString(reader, allocator);
        }
        if (property_flags & 0x0100 != 0) { // expiration
            properties.expiration = try readShortString(reader, allocator);
        }
        if (property_flags & 0x0080 != 0) { // message-id
            properties.message_id = try readShortString(reader, allocator);
        }
        if (property_flags & 0x0040 != 0) { // timestamp
            properties.timestamp = try reader.readInt(u64, .big);
        }
        if (property_flags & 0x0020 != 0) { // type
            properties.type = try readShortString(reader, allocator);
        }
        if (property_flags & 0x0010 != 0) { // user-id
            properties.user_id = try readShortString(reader, allocator);
        }
        if (property_flags & 0x0008 != 0) { // app-id
            properties.app_id = try readShortString(reader, allocator);
        }
        if (property_flags & 0x0004 != 0) { // cluster-id
            properties.cluster_id = try readShortString(reader, allocator);
        }

        return properties;
    }

    pub fn encode(self: *const BasicProperties, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        // Calculate property flags
        var property_flags: u16 = 0;
        if (self.content_type != null) property_flags |= 0x8000;
        if (self.content_encoding != null) property_flags |= 0x4000;
        if (self.headers != null) property_flags |= 0x2000;
        if (self.delivery_mode != null) property_flags |= 0x1000;
        if (self.priority != null) property_flags |= 0x0800;
        if (self.correlation_id != null) property_flags |= 0x0400;
        if (self.reply_to != null) property_flags |= 0x0200;
        if (self.expiration != null) property_flags |= 0x0100;
        if (self.message_id != null) property_flags |= 0x0080;
        if (self.timestamp != null) property_flags |= 0x0040;
        if (self.type != null) property_flags |= 0x0020;
        if (self.user_id != null) property_flags |= 0x0010;
        if (self.app_id != null) property_flags |= 0x0008;
        if (self.cluster_id != null) property_flags |= 0x0004;

        // Write property flags
        try buffer.writer().writeInt(u16, property_flags, .big);

        // Write properties in order
        if (self.content_type) |ct| try writeShortString(buffer.writer(), ct);
        if (self.content_encoding) |ce| try writeShortString(buffer.writer(), ce);
        if (self.headers) |h| {
            try buffer.writer().writeInt(u32, @intCast(h.len), .big);
            try buffer.appendSlice(h);
        }
        if (self.delivery_mode) |dm| try buffer.writer().writeByte(dm);
        if (self.priority) |p| try buffer.writer().writeByte(p);
        if (self.correlation_id) |ci| try writeShortString(buffer.writer(), ci);
        if (self.reply_to) |rt| try writeShortString(buffer.writer(), rt);
        if (self.expiration) |e| try writeShortString(buffer.writer(), e);
        if (self.message_id) |mi| try writeShortString(buffer.writer(), mi);
        if (self.timestamp) |ts| try buffer.writer().writeInt(u64, ts, .big);
        if (self.type) |t| try writeShortString(buffer.writer(), t);
        if (self.user_id) |ui| try writeShortString(buffer.writer(), ui);
        if (self.app_id) |ai| try writeShortString(buffer.writer(), ai);
        if (self.cluster_id) |ci| try writeShortString(buffer.writer(), ci);

        return buffer.toOwnedSlice();
    }

    pub fn deinit(self: *const BasicProperties, allocator: std.mem.Allocator) void {
        if (self.content_type) |ct| allocator.free(ct);
        if (self.content_encoding) |ce| allocator.free(ce);
        if (self.headers) |h| allocator.free(h);
        if (self.correlation_id) |ci| allocator.free(ci);
        if (self.reply_to) |rt| allocator.free(rt);
        if (self.expiration) |e| allocator.free(e);
        if (self.message_id) |mi| allocator.free(mi);
        if (self.type) |t| allocator.free(t);
        if (self.user_id) |ui| allocator.free(ui);
        if (self.app_id) |ai| allocator.free(ai);
        if (self.cluster_id) |ci| allocator.free(ci);
    }
};

// Content Header Frame Structure
pub const ContentHeader = struct {
    class_id: u16,
    weight: u16,
    body_size: u64,
    properties: BasicProperties,

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !ContentHeader {
        if (data.len < 12) return error.InvalidContentHeader;

        var stream = std.io.fixedBufferStream(data);
        const reader = stream.reader();

        const class_id = try reader.readInt(u16, .big);
        const weight = try reader.readInt(u16, .big);
        const body_size = try reader.readInt(u64, .big);

        // Remaining data is properties
        const properties_data = data[12..];
        const properties = try BasicProperties.decode(properties_data, allocator);

        return ContentHeader{
            .class_id = class_id,
            .weight = weight,
            .body_size = body_size,
            .properties = properties,
        };
    }

    pub fn encode(self: *const ContentHeader, allocator: std.mem.Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        defer buffer.deinit();

        try buffer.writer().writeInt(u16, self.class_id, .big);
        try buffer.writer().writeInt(u16, self.weight, .big);
        try buffer.writer().writeInt(u64, self.body_size, .big);

        const properties_data = try self.properties.encode(allocator);
        defer allocator.free(properties_data);
        try buffer.appendSlice(properties_data);

        return buffer.toOwnedSlice();
    }

    pub fn deinit(self: *const ContentHeader, allocator: std.mem.Allocator) void {
        self.properties.deinit(allocator);
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

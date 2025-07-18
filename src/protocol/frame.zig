const std = @import("std");

pub const FrameType = enum(u8) {
    method = 1,
    header = 2,
    body = 3,
    heartbeat = 8,
};

pub const Frame = struct {
    frame_type: FrameType,
    channel_id: u16,
    payload: []const u8,

    const FRAME_END_MARKER: u8 = 0xCE;
    const FRAME_HEADER_SIZE: usize = 7; // type(1) + channel(2) + size(4)
    const FRAME_TOTAL_OVERHEAD: usize = 8; // header + end marker

    pub fn encode(self: *const Frame, allocator: std.mem.Allocator) ![]u8 {
        const frame_size = self.payload.len;
        var buffer = try allocator.alloc(u8, FRAME_TOTAL_OVERHEAD + frame_size);

        buffer[0] = @intFromEnum(self.frame_type);
        std.mem.writeInt(u16, buffer[1..3], self.channel_id, .big);
        std.mem.writeInt(u32, buffer[3..7], @intCast(frame_size), .big);
        @memcpy(buffer[7 .. 7 + frame_size], self.payload);
        buffer[7 + frame_size] = FRAME_END_MARKER;

        return buffer;
    }

    pub fn decode(data: []const u8, allocator: std.mem.Allocator) !Frame {
        if (data.len < FRAME_TOTAL_OVERHEAD) return error.InvalidFrame;

        const frame_type = @as(FrameType, @enumFromInt(data[0])) catch return error.InvalidFrameType;
        const channel_id = std.mem.readInt(u16, data[1..3], .big);
        const frame_size = std.mem.readInt(u32, data[3..7], .big);

        if (data.len < FRAME_TOTAL_OVERHEAD + frame_size) return error.IncompleteFrame;
        if (data[7 + frame_size] != FRAME_END_MARKER) return error.InvalidFrameEnd;

        const payload = try allocator.dupe(u8, data[7 .. 7 + frame_size]);

        return Frame{
            .frame_type = frame_type,
            .channel_id = channel_id,
            .payload = payload,
        };
    }

    pub fn createMethod(channel_id: u16, payload: []const u8) Frame {
        return Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = payload,
        };
    }

    pub fn createHeader(channel_id: u16, payload: []const u8) Frame {
        return Frame{
            .frame_type = .header,
            .channel_id = channel_id,
            .payload = payload,
        };
    }

    pub fn createBody(channel_id: u16, payload: []const u8) Frame {
        return Frame{
            .frame_type = .body,
            .channel_id = channel_id,
            .payload = payload,
        };
    }

    pub fn createHeartbeat() Frame {
        return Frame{
            .frame_type = .heartbeat,
            .channel_id = 0,
            .payload = &[_]u8{},
        };
    }

    pub fn deinit(self: *const Frame, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
    }

    pub fn getFrameSize(data: []const u8) !u32 {
        if (data.len < FRAME_HEADER_SIZE) return error.InvalidFrame;
        return std.mem.readInt(u32, data[3..7], .big);
    }

    pub fn isComplete(data: []const u8) bool {
        if (data.len < FRAME_HEADER_SIZE) return false;
        const frame_size = std.mem.readInt(u32, data[3..7], .big);
        return data.len >= FRAME_TOTAL_OVERHEAD + frame_size;
    }
};

// Frame validation
pub fn validateFrame(frame: *const Frame) !void {
    switch (frame.frame_type) {
        .method => {
            if (frame.payload.len < 4) return error.InvalidMethodFrame;
            // Method frames should have class_id and method_id
        },
        .header => {
            if (frame.payload.len < 12) return error.InvalidHeaderFrame;
            // Header frames should have class_id, weight, body_size, and properties
        },
        .body => {
            // Body frames can have any payload size
        },
        .heartbeat => {
            if (frame.channel_id != 0) return error.InvalidHeartbeatChannel;
            if (frame.payload.len != 0) return error.InvalidHeartbeatPayload;
        },
    }
}

test "frame encoding and decoding" {
    const allocator = std.testing.allocator;

    const payload = "test payload";
    const original_frame = Frame{
        .frame_type = .method,
        .channel_id = 1,
        .payload = payload,
    };

    const encoded = try original_frame.encode(allocator);
    defer allocator.free(encoded);

    const decoded = try Frame.decode(encoded, allocator);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(original_frame.frame_type, decoded.frame_type);
    try std.testing.expectEqual(original_frame.channel_id, decoded.channel_id);
    try std.testing.expectEqualStrings(original_frame.payload, decoded.payload);
}

test "heartbeat frame" {
    const allocator = std.testing.allocator;

    const heartbeat = Frame.createHeartbeat();
    const encoded = try heartbeat.encode(allocator);
    defer allocator.free(encoded);

    const decoded = try Frame.decode(encoded, allocator);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(FrameType.heartbeat, decoded.frame_type);
    try std.testing.expectEqual(@as(u16, 0), decoded.channel_id);
    try std.testing.expectEqual(@as(usize, 0), decoded.payload.len);
}

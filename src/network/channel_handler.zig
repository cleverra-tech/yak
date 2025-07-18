const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;

/// Handles AMQP channel-level methods (Channel.Open, Close, Flow)
pub const ChannelHandler = struct {
    allocator: std.mem.Allocator,
    error_handler_fn: ?*const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction,

    pub fn init(allocator: std.mem.Allocator) ChannelHandler {
        return ChannelHandler{
            .allocator = allocator,
            .error_handler_fn = null,
        };
    }

    pub fn setErrorHandler(self: *ChannelHandler, error_handler_fn: *const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction) void {
        self.error_handler_fn = error_handler_fn;
    }

    pub fn handleChannelOpen(self: *ChannelHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = payload; // Channel.Open has no parameters in AMQP 0-9-1

        if (connection.getChannel(channel_id) != null) {
            const error_info = ErrorHelpers.channelError(.channel_error, "Channel already open", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.ChannelAlreadyOpen;
        }

        // Create new channel
        try connection.addChannel(channel_id);

        // Send Channel.OpenOk
        try self.sendChannelOpenOk(connection, channel_id);

        std.log.debug("Channel {} opened on connection {}", .{ channel_id, connection.id });
    }

    pub fn sendChannelOpenOk(self: *ChannelHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Channel = 20)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 20)));
        // Method ID (OpenOk = 11)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 11)));

        // Reserved field (long string) - empty
        try payload.appendSlice(&std.mem.toBytes(@as(u32, 0)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Channel.OpenOk sent to channel {} on connection {}", .{ channel_id, connection.id });
    }

    pub fn handleChannelFlow(self: *ChannelHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        if (payload.len < 1) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Channel.Flow payload", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidChannelFlow;
        }

        const active = payload[0] != 0;

        const channel = connection.getChannel(channel_id);
        if (channel == null) {
            const error_info = ErrorHelpers.channelError(.channel_error, "Channel not open", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.ChannelNotOpen;
        }

        channel.?.flow_active = active;

        // Send Channel.FlowOk
        try self.sendChannelFlowOk(connection, channel_id, active);

        std.log.debug("Channel {} flow set to {} on connection {}", .{ channel_id, active, connection.id });
    }

    pub fn sendChannelFlowOk(self: *ChannelHandler, connection: *Connection, channel_id: u16, active: bool) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Channel = 20)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 20)));
        // Method ID (FlowOk = 21)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 21)));

        // Active
        try payload.append(if (active) 1 else 0);

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Channel.FlowOk sent to channel {} on connection {}: active={}", .{ channel_id, connection.id, active });
    }

    pub fn handleChannelClose(self: *ChannelHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        _ = payload; // TODO: Parse close reason

        connection.removeChannel(channel_id);

        // Send Channel.CloseOk
        try self.sendChannelCloseOk(connection, channel_id);

        std.log.debug("Channel {} closed on connection {}", .{ channel_id, connection.id });
    }

    pub fn sendChannelCloseOk(self: *ChannelHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Channel = 20)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 20)));
        // Method ID (CloseOk = 41)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 41)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
        std.log.debug("Channel.CloseOk sent to channel {} on connection {}", .{ channel_id, connection.id });
    }
};

test "channel handler creation" {
    const allocator = std.testing.allocator;

    const handler = ChannelHandler.init(allocator);
    try std.testing.expect(handler.error_handler_fn == null);
}

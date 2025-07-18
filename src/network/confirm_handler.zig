const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const Methods = @import("../protocol/methods.zig");
const ConfirmMethod = Methods.ConfirmMethod;
const ConfirmSelect = Methods.ConfirmSelect;
const ConfirmSelectOk = Methods.ConfirmSelectOk;
const BasicAck = Methods.BasicAck;
const BasicNack = Methods.BasicNack;
const ClassId = Methods.ClassId;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const ErrorInfo = @import("../error/error_handler.zig").ErrorInfo;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;

pub const ConfirmHandler = struct {
    allocator: std.mem.Allocator,
    error_handler_fn: ?*const fn (error_info: ErrorInfo) RecoveryAction,

    pub fn init(allocator: std.mem.Allocator, error_handler_fn: ?*const fn (error_info: ErrorInfo) RecoveryAction) ConfirmHandler {
        return ConfirmHandler{
            .allocator = allocator,
            .error_handler_fn = error_handler_fn,
        };
    }

    pub fn handleConfirmMethod(self: *ConfirmHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        const method = @as(ConfirmMethod, @enumFromInt(method_id));

        switch (method) {
            .select => try self.handleConfirmSelect(connection, channel_id, payload),
            .select_ok => {
                // Client should not send ConfirmSelectOk to server
                std.log.warn("Unexpected ConfirmSelectOk from client on channel {}", .{channel_id});
            },
        }
    }

    pub fn handleConfirmSelect(self: *ConfirmHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        const confirm_select = ConfirmSelect.decode(payload) catch |err| {
            std.log.warn("Failed to decode ConfirmSelect: {}", .{err});
            return self.handleError(ErrorHelpers.recoverableError(.syntax_error, "Invalid ConfirmSelect method"));
        };

        // Get channel
        const channel = connection.channels.getPtr(channel_id) orelse {
            std.log.warn("ConfirmSelect received on non-existent channel {}", .{channel_id});
            return self.handleError(ErrorHelpers.channelError(.channel_error, "Channel not found", connection.id, channel_id));
        };

        // Enable confirm mode
        channel.*.enableConfirmMode() catch |err| switch (err) {
            error.TransactionModeEnabled => {
                std.log.warn("Cannot enable confirm mode: transaction mode already enabled on channel {}", .{channel_id});
                return self.handleError(ErrorHelpers.channelError(.precondition_failed, "Transaction mode already enabled", connection.id, channel_id));
            },
            else => return err,
        };

        std.log.info("Confirm mode enabled on channel {}", .{channel_id});

        // Send ConfirmSelectOk response if not nowait
        if (!confirm_select.nowait) {
            try self.sendConfirmSelectOk(connection, channel_id);
        }
    }

    pub fn sendConfirmSelectOk(self: *ConfirmHandler, connection: *Connection, channel_id: u16) !void {
        const confirm_select_ok = ConfirmSelectOk{};
        const method_payload = try confirm_select_ok.encode(self.allocator);
        defer self.allocator.free(method_payload);

        const method_frame = Methods.MethodFrame{
            .class_id = @intFromEnum(ClassId.confirm),
            .method_id = @intFromEnum(ConfirmMethod.select_ok),
            .arguments = method_payload,
        };

        const frame_payload = try method_frame.encode(self.allocator);
        defer self.allocator.free(frame_payload);

        const frame = Frame{
            .frame_type = FrameType.method,
            .channel_id = channel_id,
            .payload = frame_payload,
        };

        try connection.sendFrame(frame);
    }

    pub fn sendBasicAck(self: *ConfirmHandler, connection: *Connection, channel_id: u16, delivery_tag: u64, multiple: bool) !void {
        const basic_ack = BasicAck{
            .delivery_tag = delivery_tag,
            .multiple = multiple,
        };

        const method_payload = try basic_ack.encode(self.allocator);
        defer self.allocator.free(method_payload);

        const method_frame = Methods.MethodFrame{
            .class_id = @intFromEnum(ClassId.basic),
            .method_id = @intFromEnum(Methods.BasicMethod.ack),
            .arguments = method_payload,
        };

        const frame_payload = try method_frame.encode(self.allocator);
        defer self.allocator.free(frame_payload);

        const frame = Frame{
            .frame_type = FrameType.method,
            .channel_id = channel_id,
            .payload = frame_payload,
        };

        try connection.sendFrame(frame);

        // Update channel state
        if (connection.channels.getPtr(channel_id)) |channel| {
            channel.*.confirmMessage(delivery_tag, multiple);
        }
    }

    pub fn sendBasicNack(self: *ConfirmHandler, connection: *Connection, channel_id: u16, delivery_tag: u64, multiple: bool) !void {
        const basic_nack = BasicNack{
            .delivery_tag = delivery_tag,
            .multiple = multiple,
            .requeue = false, // Not used in publisher confirms
        };

        const method_payload = try basic_nack.encode(self.allocator);
        defer self.allocator.free(method_payload);

        const method_frame = Methods.MethodFrame{
            .class_id = @intFromEnum(ClassId.basic),
            .method_id = @intFromEnum(Methods.BasicMethod.nack),
            .arguments = method_payload,
        };

        const frame_payload = try method_frame.encode(self.allocator);
        defer self.allocator.free(frame_payload);

        const frame = Frame{
            .frame_type = FrameType.method,
            .channel_id = channel_id,
            .payload = frame_payload,
        };

        try connection.sendFrame(frame);

        // Update channel state
        if (connection.channels.getPtr(channel_id)) |channel| {
            channel.*.confirmMessage(delivery_tag, multiple);
        }
    }

    fn handleError(self: *ConfirmHandler, error_info: ErrorInfo) !void {
        if (self.error_handler_fn) |handler| {
            const action = handler(error_info);
            switch (action) {
                .continue_operation => return,
                .close_connection => return error.ConnectionClosed,
                .close_channel => return error.ChannelClosed,
                .restart_connection => return error.RestartRequired,
                .shutdown_server => return error.ShutdownRequired,
            }
        }
        return error.ProtocolError;
    }
};

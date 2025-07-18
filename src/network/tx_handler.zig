const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const Methods = @import("../protocol/methods.zig");
const TxMethod = Methods.TxMethod;
const TxSelect = Methods.TxSelect;
const TxSelectOk = Methods.TxSelectOk;
const TxCommit = Methods.TxCommit;
const TxCommitOk = Methods.TxCommitOk;
const TxRollback = Methods.TxRollback;
const TxRollbackOk = Methods.TxRollbackOk;
const ClassId = Methods.ClassId;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const ErrorInfo = @import("../error/error_handler.zig").ErrorInfo;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;

pub const TxHandler = struct {
    allocator: std.mem.Allocator,
    error_handler_fn: ?*const fn (error_info: ErrorInfo) RecoveryAction,

    pub fn init(allocator: std.mem.Allocator, error_handler_fn: ?*const fn (error_info: ErrorInfo) RecoveryAction) TxHandler {
        return TxHandler{
            .allocator = allocator,
            .error_handler_fn = error_handler_fn,
        };
    }

    pub fn handleTxMethod(self: *TxHandler, connection: *Connection, channel_id: u16, method_id: u16, payload: []const u8) !void {
        const method = @as(TxMethod, @enumFromInt(method_id));

        switch (method) {
            .select => try self.handleTxSelect(connection, channel_id, payload),
            .select_ok => {
                // Client should not send TxSelectOk to server
                std.log.warn("Unexpected TxSelectOk from client on channel {}", .{channel_id});
            },
            .commit => try self.handleTxCommit(connection, channel_id, payload),
            .commit_ok => {
                // Client should not send TxCommitOk to server
                std.log.warn("Unexpected TxCommitOk from client on channel {}", .{channel_id});
            },
            .rollback => try self.handleTxRollback(connection, channel_id, payload),
            .rollback_ok => {
                // Client should not send TxRollbackOk to server
                std.log.warn("Unexpected TxRollbackOk from client on channel {}", .{channel_id});
            },
        }
    }

    pub fn handleTxSelect(self: *TxHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        const tx_select = TxSelect.decode(payload) catch |err| {
            std.log.warn("Failed to decode TxSelect: {}", .{err});
            return self.handleError(ErrorHelpers.recoverableError(.syntax_error, "Invalid TxSelect method"));
        };
        _ = tx_select; // No arguments

        // Get channel
        const channel = connection.channels.getPtr(channel_id) orelse {
            std.log.warn("TxSelect received on non-existent channel {}", .{channel_id});
            return self.handleError(ErrorHelpers.channelError(.channel_error, "Channel not found", connection.id, channel_id));
        };

        // Enable transaction mode
        channel.*.enableTransactionMode() catch |err| switch (err) {
            error.ConfirmModeEnabled => {
                std.log.warn("Cannot enable transaction mode: confirm mode already enabled on channel {}", .{channel_id});
                return self.handleError(ErrorHelpers.channelError(.precondition_failed, "Confirm mode already enabled", connection.id, channel_id));
            },
            else => return err,
        };

        std.log.info("Transaction mode enabled on channel {}", .{channel_id});

        // Send TxSelectOk response
        try self.sendTxSelectOk(connection, channel_id);
    }

    pub fn handleTxCommit(self: *TxHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        const tx_commit = TxCommit.decode(payload) catch |err| {
            std.log.warn("Failed to decode TxCommit: {}", .{err});
            return self.handleError(ErrorHelpers.recoverableError(.syntax_error, "Invalid TxCommit method"));
        };
        _ = tx_commit; // No arguments

        // Get channel
        const channel = connection.channels.getPtr(channel_id) orelse {
            std.log.warn("TxCommit received on non-existent channel {}", .{channel_id});
            return self.handleError(ErrorHelpers.channelError(.channel_error, "Channel not found", connection.id, channel_id));
        };

        // Check if in transaction mode
        if (!channel.*.tx_mode) {
            std.log.warn("TxCommit received but transaction mode not enabled on channel {}", .{channel_id});
            return self.handleError(ErrorHelpers.channelError(.precondition_failed, "Transaction mode not enabled", connection.id, channel_id));
        }

        // Commit transaction
        channel.*.commitTransaction();
        std.log.info("Transaction committed on channel {}", .{channel_id});

        // Send TxCommitOk response
        try self.sendTxCommitOk(connection, channel_id);
    }

    pub fn handleTxRollback(self: *TxHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        const tx_rollback = TxRollback.decode(payload) catch |err| {
            std.log.warn("Failed to decode TxRollback: {}", .{err});
            return self.handleError(ErrorHelpers.recoverableError(.syntax_error, "Invalid TxRollback method"));
        };
        _ = tx_rollback; // No arguments

        // Get channel
        const channel = connection.channels.getPtr(channel_id) orelse {
            std.log.warn("TxRollback received on non-existent channel {}", .{channel_id});
            return self.handleError(ErrorHelpers.channelError(.channel_error, "Channel not found", connection.id, channel_id));
        };

        // Check if in transaction mode
        if (!channel.*.tx_mode) {
            std.log.warn("TxRollback received but transaction mode not enabled on channel {}", .{channel_id});
            return self.handleError(ErrorHelpers.channelError(.precondition_failed, "Transaction mode not enabled", connection.id, channel_id));
        }

        // Rollback transaction
        channel.*.rollbackTransaction();
        std.log.info("Transaction rolled back on channel {}", .{channel_id});

        // Send TxRollbackOk response
        try self.sendTxRollbackOk(connection, channel_id);
    }

    pub fn sendTxSelectOk(self: *TxHandler, connection: *Connection, channel_id: u16) !void {
        const tx_select_ok = TxSelectOk{};
        const method_payload = try tx_select_ok.encode(self.allocator);
        defer self.allocator.free(method_payload);

        const method_frame = Methods.MethodFrame{
            .class_id = @intFromEnum(ClassId.tx),
            .method_id = @intFromEnum(TxMethod.select_ok),
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

    pub fn sendTxCommitOk(self: *TxHandler, connection: *Connection, channel_id: u16) !void {
        const tx_commit_ok = TxCommitOk{};
        const method_payload = try tx_commit_ok.encode(self.allocator);
        defer self.allocator.free(method_payload);

        const method_frame = Methods.MethodFrame{
            .class_id = @intFromEnum(ClassId.tx),
            .method_id = @intFromEnum(TxMethod.commit_ok),
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

    pub fn sendTxRollbackOk(self: *TxHandler, connection: *Connection, channel_id: u16) !void {
        const tx_rollback_ok = TxRollbackOk{};
        const method_payload = try tx_rollback_ok.encode(self.allocator);
        defer self.allocator.free(method_payload);

        const method_frame = Methods.MethodFrame{
            .class_id = @intFromEnum(ClassId.tx),
            .method_id = @intFromEnum(TxMethod.rollback_ok),
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

    fn handleError(self: *TxHandler, error_info: ErrorInfo) !void {
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

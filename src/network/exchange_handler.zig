const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const VirtualHost = @import("../core/vhost.zig").VirtualHost;
const Exchange = @import("../routing/exchange.zig").Exchange;
const ExchangeType = @import("../routing/exchange.zig").ExchangeType;
const FieldTable = @import("field_table.zig").FieldTable;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;

/// Handles AMQP exchange-level methods (Exchange.Declare, Delete, Bind, Unbind)
pub const ExchangeHandler = struct {
    allocator: std.mem.Allocator,
    field_table: FieldTable,
    get_vhost_fn: ?*const fn (vhost_name: []const u8) ?*VirtualHost,
    error_handler_fn: ?*const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction,

    pub fn init(allocator: std.mem.Allocator) ExchangeHandler {
        return ExchangeHandler{
            .allocator = allocator,
            .field_table = FieldTable.init(allocator),
            .get_vhost_fn = null,
            .error_handler_fn = null,
        };
    }

    pub fn setGetVirtualHostFunction(self: *ExchangeHandler, get_vhost_fn: *const fn (vhost_name: []const u8) ?*VirtualHost) void {
        self.get_vhost_fn = get_vhost_fn;
    }

    pub fn setErrorHandler(self: *ExchangeHandler, error_handler_fn: *const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction) void {
        self.error_handler_fn = error_handler_fn;
    }

    pub fn handleExchangeDeclare(self: *ExchangeHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Declare payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDeclare;
        }
        offset += 2;

        // Parse exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Declare payload: missing exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDeclare;
        }

        const name_len = payload[offset];
        offset += 1;

        if (offset + name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Declare payload: exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDeclare;
        }

        const exchange_name = payload[offset .. offset + name_len];
        offset += name_len;

        // Parse exchange type (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Declare payload: missing exchange type length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDeclare;
        }

        const type_len = payload[offset];
        offset += 1;

        if (offset + type_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Declare payload: exchange type data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDeclare;
        }

        const exchange_type_str = payload[offset .. offset + type_len];
        offset += type_len;

        // Parse flags (1 byte: passive, durable, auto-delete, internal, no-wait)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Declare payload: missing flags", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDeclare;
        }

        const flags = payload[offset];
        offset += 1;

        const passive = (flags & 0x01) != 0;
        const durable = (flags & 0x02) != 0;
        const auto_delete = (flags & 0x04) != 0;
        const internal = (flags & 0x08) != 0;
        const no_wait = (flags & 0x10) != 0;

        // Parse arguments field table
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Declare payload: missing arguments length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDeclare;
        }

        const args_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + args_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Declare payload: arguments data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDeclare;
        }

        const arguments_data = if (args_len > 0) payload[offset .. offset + args_len] else null;

        // Parse exchange type
        const exchange_type = if (std.mem.eql(u8, exchange_type_str, "direct"))
            ExchangeType.direct
        else if (std.mem.eql(u8, exchange_type_str, "fanout"))
            ExchangeType.fanout
        else if (std.mem.eql(u8, exchange_type_str, "topic"))
            ExchangeType.topic
        else if (std.mem.eql(u8, exchange_type_str, "headers"))
            ExchangeType.headers
        else {
            const error_info = ErrorHelpers.channelError(.command_invalid, "Unsupported exchange type", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.UnsupportedExchangeType;
        };

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Handle passive mode - check if exchange exists
        if (passive) {
            if (vhost.?.getExchange(exchange_name) == null) {
                const error_info = ErrorHelpers.channelError(.not_found, "Exchange does not exist", connection.id, channel_id);
                if (self.error_handler_fn) |handle_error| {
                    _ = handle_error(error_info);
                }
                return error.ExchangeNotFound;
            }
        } else {
            // Create exchange if it doesn't exist
            vhost.?.declareExchange(exchange_name, exchange_type, durable, auto_delete, internal, arguments_data) catch |err| {
                const error_info = ErrorHelpers.channelError(.resource_error, "Failed to create exchange", connection.id, channel_id);
                if (self.error_handler_fn) |handle_error| {
                    _ = handle_error(error_info);
                }
                return err;
            };
        }

        // Send Exchange.DeclareOk if not no-wait
        if (!no_wait) {
            try self.sendExchangeDeclareOk(connection, channel_id);
        }
    }

    pub fn sendExchangeDeclareOk(self: *ExchangeHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Exchange = 40)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 40)));
        // Method ID (DeclareOk = 11)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 11)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
    }

    pub fn handleExchangeDelete(self: *ExchangeHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Delete payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDelete;
        }
        offset += 2;

        // Parse exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Delete payload: missing exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDelete;
        }

        const name_len = payload[offset];
        offset += 1;

        if (offset + name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Delete payload: exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDelete;
        }

        const exchange_name = payload[offset .. offset + name_len];
        offset += name_len;

        // Parse flags (1 byte: if-unused, no-wait)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Delete payload: missing flags", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeDelete;
        }

        const flags = payload[offset];
        const if_unused = (flags & 0x01) != 0;
        const no_wait = (flags & 0x02) != 0;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Delete exchange
        vhost.?.deleteExchange(exchange_name, if_unused) catch |err| {
            const error_info = ErrorHelpers.channelError(.precondition_failed, "Failed to delete exchange", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return err;
        };

        // Send Exchange.DeleteOk if not no-wait
        if (!no_wait) {
            try self.sendExchangeDeleteOk(connection, channel_id);
        }
    }

    pub fn sendExchangeDeleteOk(self: *ExchangeHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Exchange = 40)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 40)));
        // Method ID (DeleteOk = 21)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 21)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
    }

    pub fn handleExchangeBind(self: *ExchangeHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }
        offset += 2;

        // Parse destination exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: missing destination exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const dest_name_len = payload[offset];
        offset += 1;

        if (offset + dest_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: destination exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const destination_exchange = payload[offset .. offset + dest_name_len];
        offset += dest_name_len;

        // Parse source exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: missing source exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const src_name_len = payload[offset];
        offset += 1;

        if (offset + src_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: source exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const source_exchange = payload[offset .. offset + src_name_len];
        offset += src_name_len;

        // Parse routing key (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: missing routing key length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const routing_key_len = payload[offset];
        offset += 1;

        if (offset + routing_key_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: routing key data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const routing_key = payload[offset .. offset + routing_key_len];
        offset += routing_key_len;

        // Parse no-wait flag (1 byte)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: missing no-wait flag", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const no_wait = payload[offset] != 0;
        offset += 1;

        // Parse arguments field table
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: missing arguments length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const args_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + args_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Bind payload: arguments data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeBind;
        }

        const arguments_data = if (args_len > 0) payload[offset .. offset + args_len] else null;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Exchange-to-exchange binding is not supported in basic AMQP 0-9-1
        // This is an extension feature - for now we'll return not implemented
        _ = destination_exchange;
        _ = source_exchange;
        _ = routing_key;
        _ = arguments_data;

        const error_info = ErrorHelpers.channelError(.not_implemented, "Exchange-to-exchange binding not implemented", connection.id, channel_id);
        if (self.error_handler_fn) |handle_error| {
            _ = handle_error(error_info);
        }

        // Send Exchange.BindOk if not no-wait (even for not implemented)
        if (!no_wait) {
            try self.sendExchangeBindOk(connection, channel_id);
        }

        return error.NotImplemented;
    }

    pub fn sendExchangeBindOk(self: *ExchangeHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Exchange = 40)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 40)));
        // Method ID (BindOk = 31)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 31)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
    }

    pub fn handleExchangeUnbind(self: *ExchangeHandler, connection: *Connection, channel_id: u16, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse reserved field (2 bytes)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: missing reserved field", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }
        offset += 2;

        // Parse destination exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: missing destination exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const dest_name_len = payload[offset];
        offset += 1;

        if (offset + dest_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: destination exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const destination_exchange = payload[offset .. offset + dest_name_len];
        offset += dest_name_len;

        // Parse source exchange name (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: missing source exchange name length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const src_name_len = payload[offset];
        offset += 1;

        if (offset + src_name_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: source exchange name data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const source_exchange = payload[offset .. offset + src_name_len];
        offset += src_name_len;

        // Parse routing key (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: missing routing key length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const routing_key_len = payload[offset];
        offset += 1;

        if (offset + routing_key_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: routing key data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const routing_key = payload[offset .. offset + routing_key_len];
        offset += routing_key_len;

        // Parse no-wait flag (1 byte)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: missing no-wait flag", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const no_wait = payload[offset] != 0;
        offset += 1;

        // Parse arguments field table
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: missing arguments length", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const args_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + args_len > payload.len) {
            const error_info = ErrorHelpers.channelError(.syntax_error, "Invalid Exchange.Unbind payload: arguments data truncated", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidExchangeUnbind;
        }

        const arguments_data = if (args_len > 0) payload[offset .. offset + args_len] else null;

        // Get virtual host
        const vhost = if (self.get_vhost_fn) |get_vhost| get_vhost(connection.virtual_host orelse "/") else null;
        if (vhost == null) {
            const error_info = ErrorHelpers.channelError(.not_found, "Virtual host not found", connection.id, channel_id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.VirtualHostNotFound;
        }

        // Exchange-to-exchange unbinding is not supported in basic AMQP 0-9-1
        // This is an extension feature - for now we'll return not implemented
        _ = destination_exchange;
        _ = source_exchange;
        _ = routing_key;
        _ = arguments_data;

        const error_info = ErrorHelpers.channelError(.not_implemented, "Exchange-to-exchange unbinding not implemented", connection.id, channel_id);
        if (self.error_handler_fn) |handle_error| {
            _ = handle_error(error_info);
        }

        // Send Exchange.UnbindOk if not no-wait (even for not implemented)
        if (!no_wait) {
            try self.sendExchangeUnbindOk(connection, channel_id);
        }

        return error.NotImplemented;
    }

    pub fn sendExchangeUnbindOk(self: *ExchangeHandler, connection: *Connection, channel_id: u16) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Exchange = 40)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 40)));
        // Method ID (UnbindOk = 51)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 51)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = channel_id,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
    }
};

test "exchange handler creation" {
    const allocator = std.testing.allocator;

    const handler = ExchangeHandler.init(allocator);
    try std.testing.expect(handler.get_vhost_fn == null);
    try std.testing.expect(handler.error_handler_fn == null);
}

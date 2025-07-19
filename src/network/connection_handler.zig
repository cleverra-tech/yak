const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Frame = @import("../protocol/frame.zig").Frame;
const FrameType = @import("../protocol/frame.zig").FrameType;
const FieldTable = @import("field_table.zig").FieldTable;
const ErrorHelpers = @import("../error/error_handler.zig").ErrorHelpers;
const RecoveryAction = @import("../error/error_handler.zig").RecoveryAction;

/// Handles AMQP connection-level methods (Connection.Start, StartOk, Tune, TuneOk, Open, Close)
pub const ConnectionHandler = struct {
    allocator: std.mem.Allocator,
    server_properties: std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    server_properties_mutex: std.Thread.RwLock,
    field_table: FieldTable,
    error_handler_fn: ?*const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction,

    pub fn init(allocator: std.mem.Allocator) !ConnectionHandler {
        var handler = ConnectionHandler{
            .allocator = allocator,
            .server_properties = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .server_properties_mutex = std.Thread.RwLock{},
            .field_table = FieldTable.init(allocator),
            .error_handler_fn = null,
        };

        // Initialize server properties
        try handler.initializeServerProperties();

        return handler;
    }

    pub fn deinit(self: *ConnectionHandler) void {
        // Clean up server properties with write lock
        self.server_properties_mutex.lock();
        defer self.server_properties_mutex.unlock();

        var iterator = self.server_properties.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.server_properties.deinit();
    }

    fn initializeServerProperties(self: *ConnectionHandler) !void {
        self.server_properties_mutex.lock();
        defer self.server_properties_mutex.unlock();

        try self.server_properties.put(try self.allocator.dupe(u8, "product"), try self.allocator.dupe(u8, "Yak AMQP Message Broker"));
        try self.server_properties.put(try self.allocator.dupe(u8, "version"), try self.allocator.dupe(u8, "0.1.0"));
        try self.server_properties.put(try self.allocator.dupe(u8, "platform"), try self.allocator.dupe(u8, "Zig"));
        try self.server_properties.put(try self.allocator.dupe(u8, "copyright"), try self.allocator.dupe(u8, "Copyright (c) 2024 Cleverra Technologies"));
        try self.server_properties.put(try self.allocator.dupe(u8, "capabilities.basic.nack"), try self.allocator.dupe(u8, "true"));
        try self.server_properties.put(try self.allocator.dupe(u8, "capabilities.consumer_cancel_notify"), try self.allocator.dupe(u8, "true"));
        try self.server_properties.put(try self.allocator.dupe(u8, "capabilities.exchange_exchange_bindings"), try self.allocator.dupe(u8, "true"));
    }

    pub fn setErrorHandler(self: *ConnectionHandler, error_handler_fn: *const fn (error_info: @import("../error/error_handler.zig").ErrorInfo) RecoveryAction) void {
        self.error_handler_fn = error_handler_fn;
    }

    pub fn sendProtocolHeader(self: *ConnectionHandler, connection: *Connection) !void {
        _ = self;
        const protocol_header = "AMQP\x00\x00\x09\x01";
        try connection.socket.writeAll(protocol_header);
    }

    pub fn sendConnectionStart(self: *ConnectionHandler, connection: *Connection) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Connection = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));
        // Method ID (Start = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));

        // Version major/minor
        try payload.append(0); // Major
        try payload.append(9); // Minor

        // Server properties (AMQP field table)
        self.server_properties_mutex.lockShared();
        const properties_data = try self.field_table.encode(self.server_properties);
        self.server_properties_mutex.unlockShared();
        defer self.allocator.free(properties_data);
        try payload.appendSlice(&std.mem.toBytes(@as(u32, @intCast(properties_data.len))));
        try payload.appendSlice(properties_data);

        // Mechanisms (long string)
        const mechanisms = "PLAIN";
        try payload.appendSlice(&std.mem.toBytes(@as(u32, @intCast(mechanisms.len))));
        try payload.appendSlice(mechanisms);

        // Locales (long string)
        const locales = "en_US";
        try payload.appendSlice(&std.mem.toBytes(@as(u32, @intCast(locales.len))));
        try payload.appendSlice(locales);

        const frame = Frame{
            .frame_type = .method,
            .channel_id = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
    }

    pub fn handleConnectionStartOk(self: *ConnectionHandler, connection: *Connection, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse client properties (field table)
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.StartOk payload: missing client properties length", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidStartOk;
        }

        const properties_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + properties_len > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.StartOk payload: client properties data truncated", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidStartOk;
        }

        // Parse client properties field table
        var client_properties = if (properties_len > 0)
            self.field_table.parse(payload[offset .. offset + properties_len]) catch |err| {
                const error_info = ErrorHelpers.connectionError(.syntax_error, "Failed to parse client properties field table", connection.id);
                if (self.error_handler_fn) |handle_error| {
                    _ = handle_error(error_info);
                }
                return err;
            }
        else
            std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(self.allocator);
        defer self.field_table.free(&client_properties);

        offset += properties_len;

        // Parse mechanism (long string)
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.StartOk payload: missing mechanism length", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidStartOk;
        }

        const mechanism_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + mechanism_len > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.StartOk payload: mechanism data truncated", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidStartOk;
        }

        const mechanism = payload[offset .. offset + mechanism_len];
        offset += mechanism_len;

        // Parse response (long string)
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.StartOk payload: missing response length", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidStartOk;
        }

        const response_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + response_len > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.StartOk payload: response data truncated", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidStartOk;
        }

        const response = payload[offset .. offset + response_len];
        offset += response_len;

        // Parse locale (long string)
        if (offset + 4 > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.StartOk payload: missing locale length", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidStartOk;
        }

        const locale_len = std.mem.readInt(u32, payload[offset .. offset + 4][0..4], .big);
        offset += 4;

        if (offset + locale_len > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.StartOk payload: locale data truncated", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidStartOk;
        }

        const locale = payload[offset .. offset + locale_len];

        // Validate mechanism (currently only PLAIN is supported)
        if (!std.mem.eql(u8, mechanism, "PLAIN")) {
            const error_info = ErrorHelpers.connectionError(.access_refused, "Unsupported authentication mechanism", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.UnsupportedAuthMechanism;
        }

        // Parse PLAIN authentication response (format: \0username\0password)
        if (response.len < 3) { // At least 2 null bytes + 1 char for minimal valid auth
            const error_info = ErrorHelpers.connectionError(.access_refused, "Invalid PLAIN authentication response", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidAuthResponse;
        }

        // Find the null separators in PLAIN response
        var null_count: u32 = 0;
        var first_null: ?usize = null;
        var second_null: ?usize = null;

        for (response, 0..) |byte, i| {
            if (byte == 0) {
                null_count += 1;
                if (first_null == null) {
                    first_null = i;
                } else if (second_null == null) {
                    second_null = i;
                    break;
                }
            }
        }

        if (null_count < 2 or first_null == null or second_null == null) {
            const error_info = ErrorHelpers.connectionError(.access_refused, "Malformed PLAIN authentication response", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidAuthResponse;
        }

        // Extract authzid (authorization identity), authcid (username), and password
        const authzid = response[0..first_null.?];
        const username = response[first_null.? + 1 .. second_null.?];
        const password = response[second_null.? + 1 ..];

        // TODO: For now, accept any non-empty username/password combination
        // In a production system, this would validate against a user database
        if (username.len == 0 or password.len == 0) {
            const error_info = ErrorHelpers.connectionError(.access_refused, "Empty username or password", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidCredentials;
        }

        // Log connection info (be careful not to log sensitive data)
        std.log.info("Connection {} authenticated: user={s}, mechanism={s}, locale={s}, authzid={s}", .{ connection.id, username, mechanism, locale, authzid });

        // Log client properties for debugging
        if (client_properties.count() > 0) {
            var prop_iter = client_properties.iterator();
            while (prop_iter.next()) |entry| {
                _ = entry;
            }
        }

        connection.setState(.start_ok_received);

        // Send Connection.Tune
        try self.sendConnectionTune(connection);
        connection.setState(.tune_sent);
    }

    pub fn sendConnectionTune(self: *ConnectionHandler, connection: *Connection) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Connection = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));
        // Method ID (Tune = 30)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 30)));

        // Channel max
        try payload.appendSlice(&std.mem.toBytes(@as(u16, connection.channel_max)));
        // Frame max
        try payload.appendSlice(&std.mem.toBytes(@as(u32, connection.max_frame_size)));
        // Heartbeat
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 60))); // 60 seconds default

        const frame = Frame{
            .frame_type = .method,
            .channel_id = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
    }

    pub fn handleConnectionTuneOk(self: *ConnectionHandler, connection: *Connection, payload: []const u8) !void {
        _ = self;

        if (payload.len < 8) {
            return error.InvalidTuneOk;
        }

        const channel_max = std.mem.readInt(u16, payload[0..2], .big);
        const frame_max = std.mem.readInt(u32, payload[2..6], .big);
        const heartbeat = std.mem.readInt(u16, payload[6..8], .big);

        // Update connection properties
        connection.channel_max = channel_max;
        connection.max_frame_size = frame_max;
        connection.heartbeat_interval = heartbeat;

        connection.setState(.tune_ok_received);
    }

    pub fn handleConnectionOpen(self: *ConnectionHandler, connection: *Connection, payload: []const u8) !void {
        if (payload.len < 5) {
            return error.InvalidOpen;
        }

        // Parse virtual host name (short string)
        const vhost_len = payload[0];
        if (1 + vhost_len > payload.len) {
            return error.InvalidOpen;
        }

        const vhost_name = payload[1 .. 1 + vhost_len];

        // TODO: For now, we'll accept any virtual host name
        // In a production system, this would validate against configured vhosts
        try connection.setVirtualHost(vhost_name);
        connection.setState(.open);

        // Send Connection.OpenOk
        try self.sendConnectionOpenOk(connection);

        std.log.info("Connection {} opened to virtual host: {s}", .{ connection.id, vhost_name });
    }

    pub fn sendConnectionOpenOk(self: *ConnectionHandler, connection: *Connection) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Connection = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));
        // Method ID (OpenOk = 41)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 41)));

        // Known hosts (short string) - empty for now
        try payload.append(0);

        const frame = Frame{
            .frame_type = .method,
            .channel_id = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
    }

    pub fn handleConnectionClose(self: *ConnectionHandler, connection: *Connection, payload: []const u8) !void {
        var offset: usize = 0;

        // Parse close reason (Connection.Close method format: reply_code, reply_text, class_id, method_id)
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.Close payload: missing reply code", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidConnectionClose;
        }

        const reply_code = std.mem.readInt(u16, payload[offset .. offset + 2][0..2], .big);
        offset += 2;

        // Parse reply text (short string)
        if (offset >= payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.Close payload: missing reply text length", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidConnectionClose;
        }

        const reply_text_len = payload[offset];
        offset += 1;

        if (offset + reply_text_len > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.Close payload: reply text data truncated", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidConnectionClose;
        }

        const reply_text = payload[offset .. offset + reply_text_len];
        offset += reply_text_len;

        // Parse class ID
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.Close payload: missing class ID", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidConnectionClose;
        }

        const class_id = std.mem.readInt(u16, payload[offset .. offset + 2][0..2], .big);
        offset += 2;

        // Parse method ID
        if (offset + 2 > payload.len) {
            const error_info = ErrorHelpers.connectionError(.syntax_error, "Invalid Connection.Close payload: missing method ID", connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
            return error.InvalidConnectionClose;
        }

        const method_id = std.mem.readInt(u16, payload[offset .. offset + 2][0..2], .big);

        // Log connection close details
        std.log.info("Connection {} closing: code={}, reason={s}, class={}, method={}", .{ connection.id, reply_code, reply_text, class_id, method_id });

        // Log error-level message if reply code indicates an error
        if (reply_code != 200) { // 200 is normal closure
            std.log.err("Connection {} closed with error: code={}, reason={s}", .{ connection.id, reply_code, reply_text });

            // Report error through error handler
            const error_msg = std.fmt.allocPrint(self.allocator, "Connection closed by client: {s}", .{reply_text}) catch "Connection closed by client";
            defer if (error_msg.ptr != "Connection closed by client".ptr) self.allocator.free(error_msg);

            const error_info = ErrorHelpers.connectionError(.connection_forced, error_msg, connection.id);
            if (self.error_handler_fn) |handle_error| {
                _ = handle_error(error_info);
            }
        }

        connection.setState(.closing);

        // Send Connection.CloseOk
        try self.sendConnectionCloseOk(connection);
        connection.setState(.closed);
    }

    pub fn sendConnectionCloseOk(self: *ConnectionHandler, connection: *Connection) !void {
        var payload = std.ArrayList(u8).init(self.allocator);
        defer payload.deinit();

        // Class ID (Connection = 10)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 10)));
        // Method ID (CloseOk = 51)
        try payload.appendSlice(&std.mem.toBytes(@as(u16, 51)));

        const frame = Frame{
            .frame_type = .method,
            .channel_id = 0,
            .payload = try self.allocator.dupe(u8, payload.items),
        };
        defer self.allocator.free(frame.payload);

        try connection.sendFrame(frame);
    }
};

test "connection handler creation" {
    const allocator = std.testing.allocator;

    var handler = try ConnectionHandler.init(allocator);
    defer handler.deinit();

    try std.testing.expect(handler.server_properties.count() > 0);
    try std.testing.expect(handler.server_properties.contains("product"));
    try std.testing.expect(handler.server_properties.contains("version"));
}

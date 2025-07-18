const std = @import("std");

/// AMQP Protocol Error Codes (defined in AMQP 0-9-1 specification)
pub const AMQPErrorCode = enum(u16) {
    // Connection errors
    connection_forced = 320,
    invalid_path = 402,
    access_refused = 403,
    not_found = 404,
    resource_locked = 405,
    precondition_failed = 406,
    
    // Channel errors  
    content_too_large = 311,
    no_route = 312,
    no_consumers = 313,
    
    // General errors
    syntax_error = 502,
    command_invalid = 503,
    channel_error = 504,
    unexpected_frame = 505,
    resource_error = 506,
    not_allowed = 530,
    not_implemented = 540,
    internal_error = 541,

    pub fn toString(self: AMQPErrorCode) []const u8 {
        return switch (self) {
            .connection_forced => "CONNECTION_FORCED",
            .invalid_path => "INVALID_PATH", 
            .access_refused => "ACCESS_REFUSED",
            .not_found => "NOT_FOUND",
            .resource_locked => "RESOURCE_LOCKED",
            .precondition_failed => "PRECONDITION_FAILED",
            .content_too_large => "CONTENT_TOO_LARGE",
            .no_route => "NO_ROUTE", 
            .no_consumers => "NO_CONSUMERS",
            .syntax_error => "SYNTAX_ERROR",
            .command_invalid => "COMMAND_INVALID",
            .channel_error => "CHANNEL_ERROR",
            .unexpected_frame => "UNEXPECTED_FRAME",
            .resource_error => "RESOURCE_ERROR",
            .not_allowed => "NOT_ALLOWED",
            .not_implemented => "NOT_IMPLEMENTED",
            .internal_error => "INTERNAL_ERROR",
        };
    }
};

/// Error severity levels for recovery decisions
pub const ErrorSeverity = enum {
    recoverable,    // Can continue operation
    connection,     // Must close connection but server continues  
    fatal,          // Must shutdown server

    pub fn toString(self: ErrorSeverity) []const u8 {
        return switch (self) {
            .recoverable => "RECOVERABLE",
            .connection => "CONNECTION", 
            .fatal => "FATAL",
        };
    }
};

/// Structured error information
pub const ErrorInfo = struct {
    code: AMQPErrorCode,
    severity: ErrorSeverity,
    message: []const u8,
    connection_id: ?u64 = null,
    channel_id: ?u16 = null,
    context: ?[]const u8 = null,
    timestamp: i64,

    pub fn init(code: AMQPErrorCode, severity: ErrorSeverity, message: []const u8) ErrorInfo {
        return ErrorInfo{
            .code = code,
            .severity = severity,
            .message = message,
            .timestamp = std.time.timestamp(),
        };
    }

    pub fn withConnection(self: ErrorInfo, connection_id: u64) ErrorInfo {
        var info = self;
        info.connection_id = connection_id;
        return info;
    }

    pub fn withChannel(self: ErrorInfo, channel_id: u16) ErrorInfo {
        var info = self;
        info.channel_id = channel_id;
        return info;
    }

    pub fn withContext(self: ErrorInfo, allocator: std.mem.Allocator, context: []const u8) !ErrorInfo {
        var info = self;
        info.context = try allocator.dupe(u8, context);
        return info;
    }

    pub fn deinit(self: *ErrorInfo, allocator: std.mem.Allocator) void {
        if (self.context) |context| {
            allocator.free(context);
        }
    }
};

/// Error recovery action to take
pub const RecoveryAction = enum {
    continue_operation,
    close_channel,
    close_connection,
    restart_connection,
    shutdown_server,

    pub fn toString(self: RecoveryAction) []const u8 {
        return switch (self) {
            .continue_operation => "CONTINUE", 
            .close_channel => "CLOSE_CHANNEL",
            .close_connection => "CLOSE_CONNECTION",
            .restart_connection => "RESTART_CONNECTION", 
            .shutdown_server => "SHUTDOWN_SERVER",
        };
    }
};

/// Comprehensive error handler with recovery logic
pub const ErrorHandler = struct {
    allocator: std.mem.Allocator,
    error_log: std.ArrayList(ErrorInfo),
    error_mutex: std.Thread.Mutex,
    error_count: std.atomic.Value(u64),
    fatal_error_count: std.atomic.Value(u64),

    pub fn init(allocator: std.mem.Allocator) ErrorHandler {
        return ErrorHandler{
            .allocator = allocator,
            .error_log = std.ArrayList(ErrorInfo).init(allocator),
            .error_mutex = std.Thread.Mutex{},
            .error_count = std.atomic.Value(u64).init(0),
            .fatal_error_count = std.atomic.Value(u64).init(0),
        };
    }

    pub fn deinit(self: *ErrorHandler) void {
        self.error_mutex.lock();
        defer self.error_mutex.unlock();

        for (self.error_log.items) |*error_info| {
            error_info.deinit(self.allocator);
        }
        self.error_log.deinit();
    }

    /// Record and handle an error, returning the recommended recovery action
    pub fn handleError(self: *ErrorHandler, error_info: ErrorInfo) RecoveryAction {
        // Log the error
        self.logError(error_info);
        
        // Increment counters
        _ = self.error_count.fetchAdd(1, .monotonic);
        if (error_info.severity == .fatal) {
            _ = self.fatal_error_count.fetchAdd(1, .monotonic);
        }

        // Determine recovery action based on error code and severity
        return self.determineRecoveryAction(error_info);
    }

    fn logError(self: *ErrorHandler, error_info: ErrorInfo) void {
        // Log to console immediately
        self.logToConsole(error_info);
        
        // Store in error log for later analysis (thread-safe)
        self.error_mutex.lock();
        defer self.error_mutex.unlock();
        
        // Prevent unbounded growth of error log
        if (self.error_log.items.len >= 1000) {
            var oldest = self.error_log.orderedRemove(0);
            oldest.deinit(self.allocator);
        }
        
        self.error_log.append(error_info) catch |err| {
            std.log.err("Failed to store error in log: {}", .{err});
        };
    }

    fn logToConsole(self: *ErrorHandler, error_info: ErrorInfo) void {
        
        const connection_info = if (error_info.connection_id) |conn_id|
            std.fmt.allocPrint(self.allocator, " connection={}", .{conn_id}) catch ""
        else
            "";
        defer if (connection_info.len > 0) self.allocator.free(connection_info);

        const channel_info = if (error_info.channel_id) |ch_id|
            std.fmt.allocPrint(self.allocator, " channel={}", .{ch_id}) catch ""
        else
            "";
        defer if (channel_info.len > 0) self.allocator.free(channel_info);

        const context_info = if (error_info.context) |ctx|
            std.fmt.allocPrint(self.allocator, " context={s}", .{ctx}) catch ""
        else
            "";
        defer if (context_info.len > 0) self.allocator.free(context_info);

        switch (error_info.severity) {
            .recoverable => std.log.warn("[{}] {s}: {s}{s}{s}{s}", .{
                @intFromEnum(error_info.code), error_info.code.toString(), error_info.message,
                connection_info, channel_info, context_info
            }),
            .connection => std.log.err("[{}] {s}: {s}{s}{s}{s}", .{
                @intFromEnum(error_info.code), error_info.code.toString(), error_info.message,
                connection_info, channel_info, context_info
            }),
            .fatal => std.log.err("[FATAL][{}] {s}: {s}{s}{s}{s}", .{
                @intFromEnum(error_info.code), error_info.code.toString(), error_info.message,
                connection_info, channel_info, context_info
            }),
        }
    }

    fn determineRecoveryAction(self: *ErrorHandler, error_info: ErrorInfo) RecoveryAction {
        _ = self;
        
        // Base decision on error severity and code
        return switch (error_info.severity) {
            .recoverable => switch (error_info.code) {
                .no_route, .no_consumers => .continue_operation,
                .channel_error, .content_too_large => .close_channel,
                else => .continue_operation,
            },
            .connection => switch (error_info.code) {
                .connection_forced, .access_refused => .close_connection,
                .syntax_error, .command_invalid => .close_connection,
                .unexpected_frame => .close_connection,
                else => .close_connection,
            },
            .fatal => switch (error_info.code) {
                .resource_error, .internal_error => .shutdown_server,
                else => .close_connection,
            },
        };
    }

    /// Get error statistics for monitoring
    pub fn getErrorStats(self: *const ErrorHandler, allocator: std.mem.Allocator) !std.json.Value {
        var stats = std.json.ObjectMap.init(allocator);
        
        try stats.put("total_errors", std.json.Value{ .integer = @intCast(self.error_count.load(.monotonic)) });
        try stats.put("fatal_errors", std.json.Value{ .integer = @intCast(self.fatal_error_count.load(.monotonic)) });
        
        self.error_mutex.lock();
        defer self.error_mutex.unlock();
        
        try stats.put("recent_errors", std.json.Value{ .integer = @intCast(self.error_log.items.len) });
        
        return std.json.Value{ .object = stats };
    }

    /// Clear old errors from the log
    pub fn clearOldErrors(self: *ErrorHandler, max_age_seconds: i64) void {
        self.error_mutex.lock();
        defer self.error_mutex.unlock();
        
        const cutoff_time = std.time.timestamp() - max_age_seconds;
        var i: usize = 0;
        
        while (i < self.error_log.items.len) {
            if (self.error_log.items[i].timestamp < cutoff_time) {
                var removed = self.error_log.orderedRemove(i);
                removed.deinit(self.allocator);
            } else {
                i += 1;
            }
        }
    }
};

/// Helper functions for common error scenarios
pub const ErrorHelpers = struct {
    /// Create error for connection-level issues
    pub fn connectionError(code: AMQPErrorCode, message: []const u8, connection_id: u64) ErrorInfo {
        return ErrorInfo.init(code, .connection, message).withConnection(connection_id);
    }

    /// Create error for channel-level issues  
    pub fn channelError(code: AMQPErrorCode, message: []const u8, connection_id: u64, channel_id: u16) ErrorInfo {
        return ErrorInfo.init(code, .recoverable, message)
            .withConnection(connection_id)
            .withChannel(channel_id);
    }

    /// Create error for fatal server issues
    pub fn fatalError(code: AMQPErrorCode, message: []const u8) ErrorInfo {
        return ErrorInfo.init(code, .fatal, message);
    }

    /// Create error for recoverable issues
    pub fn recoverableError(code: AMQPErrorCode, message: []const u8) ErrorInfo {
        return ErrorInfo.init(code, .recoverable, message);
    }
};
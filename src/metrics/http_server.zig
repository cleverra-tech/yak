const std = @import("std");
const net = std.net;
const http = std.http;
const json = std.json;
const Allocator = std.mem.Allocator;
const MetricsRegistry = @import("metrics.zig").MetricsRegistry;

/// HTTP server for metrics endpoints
pub const MetricsHttpServer = struct {
    allocator: Allocator,
    registry: *MetricsRegistry,
    server: ?net.Server,
    address: net.Address,
    running: bool,
    thread: ?std.Thread,
    shutdown_requested: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(allocator: Allocator, registry: *MetricsRegistry, host: []const u8, port: u16) !Self {
        const address = try net.Address.parseIp(host, port);

        return Self{
            .allocator = allocator,
            .registry = registry,
            .server = null,
            .address = address,
            .running = false,
            .thread = null,
            .shutdown_requested = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *Self) void {
        self.stop();
        if (self.thread) |thread| {
            thread.join();
        }
    }

    pub fn start(self: *Self) !void {
        if (self.running) return;

        self.server = try self.address.listen(.{ .reuse_address = true });
        self.running = true;
        self.shutdown_requested.store(false, .monotonic);

        // Start server in separate thread
        self.thread = try std.Thread.spawn(.{}, serverLoop, .{self});

        std.log.info("Metrics HTTP server started on {any}", .{self.address});
    }

    pub fn stop(self: *Self) void {
        if (!self.running) return;

        self.running = false;
        self.shutdown_requested.store(true, .monotonic);

        if (self.server) |*server| {
            server.deinit();
            self.server = null;
        }

        std.log.info("Metrics HTTP server stopped", .{});
    }

    fn serverLoop(self: *Self) void {
        while (self.running and !self.shutdown_requested.load(.monotonic)) {
            if (self.server) |*server| {
                // Accept connections with timeout
                const client = self.acceptWithTimeout(server, 100) catch |err| switch (err) {
                    error.WouldBlock => continue,
                    else => {
                        std.log.warn("Failed to accept metrics client: {}", .{err});
                        continue;
                    },
                };

                if (client) |conn| {
                    self.handleClient(conn) catch |err| {
                        std.log.warn("Error handling metrics client: {}", .{err});
                    };
                }
            }
        }
    }

    fn acceptWithTimeout(self: *Self, server: *net.Server, timeout_ms: u32) !?net.Server.Connection {
        _ = self;
        _ = timeout_ms;

        // Use poll to check if socket is ready
        var pollfds = [_]std.posix.pollfd{
            std.posix.pollfd{
                .fd = server.stream.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            },
        };

        const poll_result = try std.posix.poll(&pollfds, 100);
        if (poll_result == 0) {
            return null; // Timeout
        }

        return try server.accept();
    }

    fn handleClient(self: *Self, connection: net.Server.Connection) !void {
        defer connection.stream.close();

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const temp_allocator = arena.allocator();

        // Read HTTP request
        var buffer: [4096]u8 = undefined;
        const bytes_read = try connection.stream.read(&buffer);

        if (bytes_read == 0) return;

        const request = buffer[0..bytes_read];

        // Parse request line
        var lines = std.mem.splitSequence(u8, request, "\r\n");
        const request_line = lines.next() orelse return;

        var parts = std.mem.splitSequence(u8, request_line, " ");
        const method = parts.next() orelse return;
        const path = parts.next() orelse return;

        // Only handle GET requests
        if (!std.mem.eql(u8, method, "GET")) {
            try self.sendResponse(connection, 405, "Method Not Allowed", "text/plain", "Method Not Allowed");
            return;
        }

        // Route requests
        if (std.mem.eql(u8, path, "/metrics")) {
            try self.handleMetricsEndpoint(connection, temp_allocator);
        } else if (std.mem.eql(u8, path, "/metrics/json")) {
            try self.handleMetricsJsonEndpoint(connection, temp_allocator);
        } else if (std.mem.eql(u8, path, "/health")) {
            try self.handleHealthEndpoint(connection);
        } else {
            try self.sendResponse(connection, 404, "Not Found", "text/plain", "Not Found");
        }
    }

    fn handleMetricsEndpoint(self: *Self, connection: net.Server.Connection, allocator: Allocator) !void {
        const prometheus_data = self.registry.getPrometheusFormat(allocator) catch |err| {
            std.log.err("Failed to generate Prometheus metrics: {}", .{err});
            try self.sendResponse(connection, 500, "Internal Server Error", "text/plain", "Internal Server Error");
            return;
        };
        defer allocator.free(prometheus_data);

        try self.sendResponse(connection, 200, "OK", "text/plain; version=0.0.4; charset=utf-8", prometheus_data);
    }

    fn handleMetricsJsonEndpoint(self: *Self, connection: net.Server.Connection, allocator: Allocator) !void {
        const metrics_json = self.registry.getAllMetrics(allocator) catch |err| {
            std.log.err("Failed to generate JSON metrics: {}", .{err});
            try self.sendResponse(connection, 500, "Internal Server Error", "text/plain", "Internal Server Error");
            return;
        };
        defer {
            var mutable_map = metrics_json.object;
            mutable_map.deinit();
        }

        const json_string = json.stringifyAlloc(allocator, metrics_json, .{}) catch |err| {
            std.log.err("Failed to stringify JSON metrics: {}", .{err});
            try self.sendResponse(connection, 500, "Internal Server Error", "text/plain", "Internal Server Error");
            return;
        };
        defer allocator.free(json_string);

        try self.sendResponse(connection, 200, "OK", "application/json", json_string);
    }

    fn handleHealthEndpoint(self: *Self, connection: net.Server.Connection) !void {
        const health_response = "{\"status\":\"ok\",\"timestamp\":\"" ++ "2024-01-01T00:00:00Z" ++ "\"}";
        try self.sendResponse(connection, 200, "OK", "application/json", health_response);
    }

    fn sendResponse(self: *Self, connection: net.Server.Connection, status_code: u16, status_text: []const u8, content_type: []const u8, body: []const u8) !void {
        const response = try std.fmt.allocPrint(self.allocator, "HTTP/1.1 {} {s}\r\n" ++
            "Content-Type: {s}\r\n" ++
            "Content-Length: {}\r\n" ++
            "Connection: close\r\n" ++
            "\r\n" ++
            "{s}", .{ status_code, status_text, content_type, body.len, body });
        defer self.allocator.free(response);

        try connection.stream.writeAll(response);
    }
};

/// Metrics collector for AMQP broker
pub const BrokerMetricsCollector = struct {
    registry: *MetricsRegistry,
    server: ?*anyopaque,
    collection_interval_ms: u64,
    collection_thread: ?std.Thread,
    running: bool,
    shutdown_requested: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(registry: *MetricsRegistry, server: ?*anyopaque, collection_interval_ms: u64) Self {
        return Self{
            .registry = registry,
            .server = server,
            .collection_interval_ms = collection_interval_ms,
            .collection_thread = null,
            .running = false,
            .shutdown_requested = std.atomic.Value(bool).init(false),
        };
    }

    pub fn deinit(self: *Self) void {
        self.stop();
        if (self.collection_thread) |thread| {
            thread.join();
        }
    }

    pub fn start(self: *Self) !void {
        if (self.running) return;

        // Register standard AMQP metrics
        try self.registerStandardMetrics();

        self.running = true;
        self.shutdown_requested.store(false, .monotonic);

        self.collection_thread = try std.Thread.spawn(.{}, collectionLoop, .{self});

        std.log.info("Metrics collection started with interval {}ms", .{self.collection_interval_ms});
    }

    pub fn stop(self: *Self) void {
        if (!self.running) return;

        self.running = false;
        self.shutdown_requested.store(true, .monotonic);

        std.log.info("Metrics collection stopped", .{});
    }

    fn registerStandardMetrics(self: *Self) !void {
        // Connection metrics
        try self.registry.register("amqp_connections_total", "Total number of AMQP connections", .gauge);
        try self.registry.register("amqp_connections_opened_total", "Total number of opened connections", .counter);
        try self.registry.register("amqp_connections_closed_total", "Total number of closed connections", .counter);

        // Channel metrics
        try self.registry.register("amqp_channels_total", "Total number of AMQP channels", .gauge);
        try self.registry.register("amqp_channels_opened_total", "Total number of opened channels", .counter);
        try self.registry.register("amqp_channels_closed_total", "Total number of closed channels", .counter);

        // Virtual host metrics
        try self.registry.register("amqp_vhosts_total", "Total number of virtual hosts", .gauge);

        // Exchange metrics
        try self.registry.register("amqp_exchanges_total", "Total number of exchanges", .gauge);
        try self.registry.register("amqp_exchanges_declared_total", "Total number of declared exchanges", .counter);
        try self.registry.register("amqp_exchanges_deleted_total", "Total number of deleted exchanges", .counter);

        // Queue metrics
        try self.registry.register("amqp_queues_total", "Total number of queues", .gauge);
        try self.registry.register("amqp_queues_declared_total", "Total number of declared queues", .counter);
        try self.registry.register("amqp_queues_deleted_total", "Total number of deleted queues", .counter);
        try self.registry.register("amqp_queue_messages_total", "Total number of messages in all queues", .gauge);
        try self.registry.register("amqp_queue_consumers_total", "Total number of consumers across all queues", .gauge);

        // Message metrics
        try self.registry.register("amqp_messages_published_total", "Total number of published messages", .counter);
        try self.registry.register("amqp_messages_delivered_total", "Total number of delivered messages", .counter);
        try self.registry.register("amqp_messages_acknowledged_total", "Total number of acknowledged messages", .counter);
        try self.registry.register("amqp_messages_rejected_total", "Total number of rejected messages", .counter);
        try self.registry.register("amqp_messages_redelivered_total", "Total number of redelivered messages", .counter);

        // Bytes metrics
        try self.registry.register("amqp_bytes_received_total", "Total bytes received", .counter);
        try self.registry.register("amqp_bytes_sent_total", "Total bytes sent", .counter);

        // Memory metrics
        try self.registry.register("amqp_memory_usage_bytes", "Memory usage in bytes", .gauge);
        try self.registry.register("amqp_queue_memory_usage_bytes", "Queue memory usage in bytes", .gauge);

        // Performance metrics
        try self.registry.register("amqp_message_processing_duration_seconds", "Message processing duration", .histogram);
        try self.registry.register("amqp_connection_processing_duration_seconds", "Connection processing duration", .histogram);

        // Error metrics
        try self.registry.register("amqp_errors_total", "Total number of errors", .counter);
        try self.registry.register("amqp_protocol_errors_total", "Total number of protocol errors", .counter);
        try self.registry.register("amqp_connection_errors_total", "Total number of connection errors", .counter);
    }

    fn collectionLoop(self: *Self) void {
        while (self.running and !self.shutdown_requested.load(.monotonic)) {
            self.collectMetrics() catch |err| {
                std.log.warn("Error collecting metrics: {}", .{err});
            };

            std.Thread.sleep(self.collection_interval_ms * std.time.ns_per_ms);
        }
    }

    fn collectMetrics(self: *Self) !void {
        if (self.server) |_| {
            // When server is available, collect basic system metrics
            // Note: Full integration requires passing server methods through interface

            // Collect memory usage
            const memory_usage = self.getMemoryUsage();
            try self.registry.setGauge("amqp_memory_usage_bytes", @floatFromInt(memory_usage));

            // Set some default values for testing
            try self.registry.setGauge("amqp_connections_total", 0);
            try self.registry.setGauge("amqp_vhosts_total", 1);
            try self.registry.setGauge("amqp_queues_total", 0);
            try self.registry.setGauge("amqp_exchanges_total", 5);
            try self.registry.setGauge("amqp_queue_messages_total", 0);
            try self.registry.setGauge("amqp_queue_consumers_total", 0);
            try self.registry.setGauge("amqp_queue_memory_usage_bytes", 0);
        }
    }

    fn getMemoryUsage(self: *Self) u64 {
        _ = self;
        // Get current memory usage from the system
        // This is a simplified implementation
        const info = std.posix.getrusage(std.posix.rusage.SELF);
        return @intCast(info.maxrss * 1024); // Convert from KB to bytes
    }
};

test "metrics HTTP server initialization" {
    const allocator = std.testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    var server = try MetricsHttpServer.init(allocator, &registry, "127.0.0.1", 8080);
    defer server.deinit();

    try std.testing.expectEqual(false, server.running);
    try std.testing.expectEqual(@as(?std.Thread, null), server.thread);
}

test "broker metrics collector initialization" {
    const allocator = std.testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    var collector = BrokerMetricsCollector.init(&registry, null, 1000);
    defer collector.deinit();

    try std.testing.expectEqual(false, collector.running);
    try std.testing.expectEqual(@as(?std.Thread, null), collector.collection_thread);
}

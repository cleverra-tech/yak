const std = @import("std");
const MetricsRegistry = @import("metrics.zig").MetricsRegistry;
const Server = @import("../core/server.zig").Server;

/// Metrics collector for AMQP broker
pub const BrokerMetricsCollector = struct {
    registry: *MetricsRegistry,
    server: *Server,
    collection_interval_ms: u64,
    collection_thread: ?std.Thread,
    running: bool,
    shutdown_requested: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(registry: *MetricsRegistry, server: *Server, collection_interval_ms: u64) Self {
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
        // Collect server metrics
        try self.registry.setGauge("amqp_connections_total", @floatFromInt(self.server.getConnectionCount()));
        try self.registry.setGauge("amqp_vhosts_total", @floatFromInt(self.server.getVirtualHostCount()));

        // Collect virtual host metrics
        var total_queues: u32 = 0;
        var total_exchanges: u32 = 0;
        var total_messages: u64 = 0;
        var total_consumers: u64 = 0;
        var total_queue_memory: u64 = 0;

        var vhost_iter = self.server.vhosts.iterator();
        while (vhost_iter.next()) |entry| {
            const vhost = entry.value_ptr.*;
            total_queues += vhost.getQueueCount();
            total_exchanges += vhost.getExchangeCount();

            // Collect queue metrics
            var queue_iter = vhost.queues.iterator();
            while (queue_iter.next()) |queue_entry| {
                const queue = queue_entry.value_ptr.*;
                total_messages += queue.getMessageCount();
                total_consumers += queue.getConsumerCount();
                total_queue_memory += queue.memory_usage;
            }
        }

        try self.registry.setGauge("amqp_queues_total", @floatFromInt(total_queues));
        try self.registry.setGauge("amqp_exchanges_total", @floatFromInt(total_exchanges));
        try self.registry.setGauge("amqp_queue_messages_total", @floatFromInt(total_messages));
        try self.registry.setGauge("amqp_queue_consumers_total", @floatFromInt(total_consumers));
        try self.registry.setGauge("amqp_queue_memory_usage_bytes", @floatFromInt(total_queue_memory));

        // Collect memory usage
        const memory_usage = self.getMemoryUsage();
        try self.registry.setGauge("amqp_memory_usage_bytes", @floatFromInt(memory_usage));
    }

    fn getMemoryUsage(self: *Self) u64 {
        _ = self;
        // Get current memory usage from the system
        // This is a simplified implementation
        const info = std.posix.getrusage(std.posix.rusage.SELF);
        return @intCast(info.maxrss * 1024); // Convert from KB to bytes
    }
};

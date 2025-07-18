const std = @import("std");
const time = std.time;
const json = std.json;
const Allocator = std.mem.Allocator;

/// Metrics error types
pub const MetricsError = error{
    InvalidMetricName,
    MetricNotFound,
    InvalidMetricType,
    OutOfMemory,
    SerializationError,
};

/// Metric data types
pub const MetricType = enum {
    counter,
    gauge,
    histogram,
    summary,
    timer,
};

/// Individual metric value
pub const MetricValue = union(MetricType) {
    counter: u64,
    gauge: f64,
    histogram: HistogramData,
    summary: SummaryData,
    timer: TimerData,
};

/// Histogram bucket data
pub const HistogramBucket = struct {
    upper_bound: f64,
    count: u64,
};

/// Histogram metric data
pub const HistogramData = struct {
    count: u64,
    sum: f64,
    buckets: []HistogramBucket,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, bucket_bounds: []const f64) !Self {
        const buckets = try allocator.alloc(HistogramBucket, bucket_bounds.len);
        for (buckets, bucket_bounds) |*bucket, bound| {
            bucket.* = HistogramBucket{
                .upper_bound = bound,
                .count = 0,
            };
        }

        return Self{
            .count = 0,
            .sum = 0.0,
            .buckets = buckets,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.buckets);
    }

    pub fn observe(self: *Self, value: f64) void {
        self.count += 1;
        self.sum += value;

        for (self.buckets) |*bucket| {
            if (value <= bucket.upper_bound) {
                bucket.count += 1;
            }
        }
    }
};

/// Summary metric data (quantiles)
pub const SummaryData = struct {
    count: u64,
    sum: f64,
    quantiles: std.HashMap(f64, f64, std.hash_map.AutoContext(f64), std.hash_map.default_max_load_percentage),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .count = 0,
            .sum = 0.0,
            .quantiles = std.HashMap(f64, f64, std.hash_map.AutoContext(f64), std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.quantiles.deinit();
    }

    pub fn observe(self: *Self, value: f64) void {
        self.count += 1;
        self.sum += value;
        // Note: In a real implementation, we would maintain a rolling window
        // and calculate quantiles. For simplicity, we'll just track count/sum.
    }
};

/// Timer metric data
pub const TimerData = struct {
    count: u64,
    sum_ns: u64,
    min_ns: u64,
    max_ns: u64,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .count = 0,
            .sum_ns = 0,
            .min_ns = std.math.maxInt(u64),
            .max_ns = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        _ = self; // No cleanup needed
    }

    pub fn record(self: *Self, duration_ns: u64) void {
        self.count += 1;
        self.sum_ns += duration_ns;
        self.min_ns = @min(self.min_ns, duration_ns);
        self.max_ns = @max(self.max_ns, duration_ns);
    }

    pub fn averageNs(self: *const Self) f64 {
        if (self.count == 0) return 0.0;
        return @as(f64, @floatFromInt(self.sum_ns)) / @as(f64, @floatFromInt(self.count));
    }

    pub fn averageMs(self: *const Self) f64 {
        return self.averageNs() / 1_000_000.0;
    }
};

/// Metric metadata
pub const Metric = struct {
    name: []const u8,
    help: []const u8,
    labels: std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    value: MetricValue,
    timestamp: i64,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, name: []const u8, help: []const u8, value: MetricValue) !Self {
        const owned_name = try allocator.dupe(u8, name);
        const owned_help = try allocator.dupe(u8, help);

        return Self{
            .name = owned_name,
            .help = owned_help,
            .labels = std.HashMap([]const u8, []const u8, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .value = value,
            .timestamp = time.timestamp(),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.name);
        self.allocator.free(self.help);

        // Clean up labels
        var label_iter = self.labels.iterator();
        while (label_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.labels.deinit();

        // Clean up value
        switch (self.value) {
            .histogram => |*hist| hist.deinit(),
            .summary => |*sum| sum.deinit(),
            .timer => |*timer| timer.deinit(),
            else => {},
        }
    }

    pub fn addLabel(self: *Self, key: []const u8, value: []const u8) !void {
        const owned_key = try self.allocator.dupe(u8, key);
        const owned_value = try self.allocator.dupe(u8, value);
        try self.labels.put(owned_key, owned_value);
    }

    pub fn updateTimestamp(self: *Self) void {
        self.timestamp = time.timestamp();
    }
};

/// Main metrics registry
pub const MetricsRegistry = struct {
    metrics: std.HashMap([]const u8, *Metric, std.hash_map.StringContext, std.hash_map.default_max_load_percentage),
    allocator: Allocator,
    mutex: std.Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .metrics = std.HashMap([]const u8, *Metric, std.hash_map.StringContext, std.hash_map.default_max_load_percentage).init(allocator),
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var iter = self.metrics.iterator();
        while (iter.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.metrics.deinit();
    }

    /// Register a new metric
    pub fn register(self: *Self, name: []const u8, help: []const u8, metric_type: MetricType) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.metrics.contains(name)) {
            return MetricsError.InvalidMetricName;
        }

        const value = switch (metric_type) {
            .counter => MetricValue{ .counter = 0 },
            .gauge => MetricValue{ .gauge = 0.0 },
            .histogram => MetricValue{ .histogram = try HistogramData.init(self.allocator, &[_]f64{ 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0 }) },
            .summary => MetricValue{ .summary = SummaryData.init(self.allocator) },
            .timer => MetricValue{ .timer = TimerData.init(self.allocator) },
        };

        const metric = try self.allocator.create(Metric);
        metric.* = try Metric.init(self.allocator, name, help, value);

        const owned_name = try self.allocator.dupe(u8, name);
        try self.metrics.put(owned_name, metric);
    }

    /// Register a histogram with custom buckets
    pub fn registerHistogram(self: *Self, name: []const u8, help: []const u8, buckets: []const f64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.metrics.contains(name)) {
            return MetricsError.InvalidMetricName;
        }

        const histogram = try HistogramData.init(self.allocator, buckets);
        const value = MetricValue{ .histogram = histogram };

        const metric = try self.allocator.create(Metric);
        metric.* = try Metric.init(self.allocator, name, help, value);

        const owned_name = try self.allocator.dupe(u8, name);
        try self.metrics.put(owned_name, metric);
    }

    /// Increment a counter
    pub fn incrementCounter(self: *Self, name: []const u8, delta: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const metric = self.metrics.get(name) orelse return MetricsError.MetricNotFound;

        switch (metric.value) {
            .counter => |*counter| {
                counter.* += delta;
                metric.updateTimestamp();
            },
            else => return MetricsError.InvalidMetricType,
        }
    }

    /// Set a gauge value
    pub fn setGauge(self: *Self, name: []const u8, value: f64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const metric = self.metrics.get(name) orelse return MetricsError.MetricNotFound;

        switch (metric.value) {
            .gauge => |*gauge| {
                gauge.* = value;
                metric.updateTimestamp();
            },
            else => return MetricsError.InvalidMetricType,
        }
    }

    /// Observe a value in histogram
    pub fn observeHistogram(self: *Self, name: []const u8, value: f64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const metric = self.metrics.get(name) orelse return MetricsError.MetricNotFound;

        switch (metric.value) {
            .histogram => |*hist| {
                hist.observe(value);
                metric.updateTimestamp();
            },
            else => return MetricsError.InvalidMetricType,
        }
    }

    /// Record a timer value
    pub fn recordTimer(self: *Self, name: []const u8, duration_ns: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const metric = self.metrics.get(name) orelse return MetricsError.MetricNotFound;

        switch (metric.value) {
            .timer => |*timer| {
                timer.record(duration_ns);
                metric.updateTimestamp();
            },
            else => return MetricsError.InvalidMetricType,
        }
    }

    /// Get metric by name
    pub fn getMetric(self: *Self, name: []const u8) ?*Metric {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.metrics.get(name);
    }

    /// Get all metrics as JSON
    pub fn getAllMetrics(self: *Self, allocator: Allocator) !json.Value {
        self.mutex.lock();
        defer self.mutex.unlock();

        var metrics_obj = json.ObjectMap.init(allocator);

        var iter = self.metrics.iterator();
        while (iter.next()) |entry| {
            const metric = entry.value_ptr.*;
            const metric_json = try self.metricToJson(metric, allocator);
            try metrics_obj.put(metric.name, metric_json);
        }

        return json.Value{ .object = metrics_obj };
    }

    /// Convert a metric to JSON
    fn metricToJson(self: *Self, metric: *Metric, allocator: Allocator) !json.Value {
        _ = self;
        var obj = json.ObjectMap.init(allocator);

        try obj.put("name", json.Value{ .string = metric.name });
        try obj.put("help", json.Value{ .string = metric.help });
        try obj.put("timestamp", json.Value{ .integer = metric.timestamp });

        // Add labels
        if (metric.labels.count() > 0) {
            var labels_obj = json.ObjectMap.init(allocator);
            var label_iter = metric.labels.iterator();
            while (label_iter.next()) |entry| {
                try labels_obj.put(entry.key_ptr.*, json.Value{ .string = entry.value_ptr.* });
            }
            try obj.put("labels", json.Value{ .object = labels_obj });
        }

        // Add value based on type
        switch (metric.value) {
            .counter => |counter| {
                try obj.put("type", json.Value{ .string = "counter" });
                try obj.put("value", json.Value{ .integer = @intCast(counter) });
            },
            .gauge => |gauge| {
                try obj.put("type", json.Value{ .string = "gauge" });
                try obj.put("value", json.Value{ .float = gauge });
            },
            .histogram => |hist| {
                try obj.put("type", json.Value{ .string = "histogram" });
                try obj.put("count", json.Value{ .integer = @intCast(hist.count) });
                try obj.put("sum", json.Value{ .float = hist.sum });

                var buckets_arr = json.Array.init(allocator);
                for (hist.buckets) |bucket| {
                    var bucket_obj = json.ObjectMap.init(allocator);
                    try bucket_obj.put("upper_bound", json.Value{ .float = bucket.upper_bound });
                    try bucket_obj.put("count", json.Value{ .integer = @intCast(bucket.count) });
                    try buckets_arr.append(json.Value{ .object = bucket_obj });
                }
                try obj.put("buckets", json.Value{ .array = buckets_arr });
            },
            .summary => |sum| {
                try obj.put("type", json.Value{ .string = "summary" });
                try obj.put("count", json.Value{ .integer = @intCast(sum.count) });
                try obj.put("sum", json.Value{ .float = sum.sum });
            },
            .timer => |timer| {
                try obj.put("type", json.Value{ .string = "timer" });
                try obj.put("count", json.Value{ .integer = @intCast(timer.count) });
                try obj.put("sum_ns", json.Value{ .integer = @intCast(timer.sum_ns) });
                try obj.put("min_ns", json.Value{ .integer = @intCast(timer.min_ns) });
                try obj.put("max_ns", json.Value{ .integer = @intCast(timer.max_ns) });
                try obj.put("average_ns", json.Value{ .float = timer.averageNs() });
                try obj.put("average_ms", json.Value{ .float = timer.averageMs() });
            },
        }

        return json.Value{ .object = obj };
    }

    /// Get metrics in Prometheus format
    pub fn getPrometheusFormat(self: *Self, allocator: Allocator) ![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        var iter = self.metrics.iterator();
        while (iter.next()) |entry| {
            const metric = entry.value_ptr.*;

            // Add help comment
            try result.appendSlice("# HELP ");
            try result.appendSlice(metric.name);
            try result.appendSlice(" ");
            try result.appendSlice(metric.help);
            try result.appendSlice("\n");

            // Add type comment
            try result.appendSlice("# TYPE ");
            try result.appendSlice(metric.name);
            try result.appendSlice(" ");

            switch (metric.value) {
                .counter => {
                    try result.appendSlice("counter\n");
                    try result.appendSlice(metric.name);
                    try result.appendSlice(" ");
                    const counter_str = try std.fmt.allocPrint(allocator, "{}", .{metric.value.counter});
                    defer allocator.free(counter_str);
                    try result.appendSlice(counter_str);
                    try result.appendSlice("\n");
                },
                .gauge => {
                    try result.appendSlice("gauge\n");
                    try result.appendSlice(metric.name);
                    try result.appendSlice(" ");
                    const gauge_str = try std.fmt.allocPrint(allocator, "{d}", .{metric.value.gauge});
                    defer allocator.free(gauge_str);
                    try result.appendSlice(gauge_str);
                    try result.appendSlice("\n");
                },
                .histogram => |hist| {
                    try result.appendSlice("histogram\n");

                    // Add bucket metrics
                    for (hist.buckets) |bucket| {
                        const bucket_line = try std.fmt.allocPrint(allocator, "{s}_bucket{{le=\"{d}\"}} {}\n", .{ metric.name, bucket.upper_bound, bucket.count });
                        defer allocator.free(bucket_line);
                        try result.appendSlice(bucket_line);
                    }

                    // Add +Inf bucket
                    const inf_bucket = try std.fmt.allocPrint(allocator, "{s}_bucket{{le=\"+Inf\"}} {}\n", .{ metric.name, hist.count });
                    defer allocator.free(inf_bucket);
                    try result.appendSlice(inf_bucket);

                    // Add count and sum
                    const count_line = try std.fmt.allocPrint(allocator, "{s}_count {}\n", .{ metric.name, hist.count });
                    defer allocator.free(count_line);
                    try result.appendSlice(count_line);

                    const sum_line = try std.fmt.allocPrint(allocator, "{s}_sum {d}\n", .{ metric.name, hist.sum });
                    defer allocator.free(sum_line);
                    try result.appendSlice(sum_line);
                },
                .timer => |timer| {
                    try result.appendSlice("summary\n");

                    const count_line = try std.fmt.allocPrint(allocator, "{s}_count {}\n", .{ metric.name, timer.count });
                    defer allocator.free(count_line);
                    try result.appendSlice(count_line);

                    const sum_line = try std.fmt.allocPrint(allocator, "{s}_sum {d}\n", .{ metric.name, timer.averageMs() });
                    defer allocator.free(sum_line);
                    try result.appendSlice(sum_line);
                },
                else => {},
            }

            try result.appendSlice("\n");
        }

        return result.toOwnedSlice();
    }
};

/// Timer helper for measuring durations
pub const Timer = struct {
    start_time: i128,

    const Self = @This();

    pub fn start() Self {
        return Self{
            .start_time = time.nanoTimestamp(),
        };
    }

    pub fn elapsed(self: *const Self) u64 {
        const now = time.nanoTimestamp();
        return @intCast(now - self.start_time);
    }

    pub fn record(self: *const Self, registry: *MetricsRegistry, metric_name: []const u8) !void {
        const duration = self.elapsed();
        try registry.recordTimer(metric_name, duration);
    }
};

// Tests
test "metrics registry basic operations" {
    const allocator = std.testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    // Test registering metrics
    try registry.register("test_counter", "A test counter", .counter);
    try registry.register("test_gauge", "A test gauge", .gauge);

    // Test counter operations
    try registry.incrementCounter("test_counter", 5);
    try registry.incrementCounter("test_counter", 3);

    const counter_metric = registry.getMetric("test_counter").?;
    try std.testing.expectEqual(@as(u64, 8), counter_metric.value.counter);

    // Test gauge operations
    try registry.setGauge("test_gauge", 42.5);

    const gauge_metric = registry.getMetric("test_gauge").?;
    try std.testing.expectEqual(@as(f64, 42.5), gauge_metric.value.gauge);
}

test "metrics histogram operations" {
    const allocator = std.testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    // Register histogram
    try registry.register("test_histogram", "A test histogram", .histogram);

    // Observe some values
    try registry.observeHistogram("test_histogram", 0.5);
    try registry.observeHistogram("test_histogram", 1.5);
    try registry.observeHistogram("test_histogram", 2.5);

    const hist_metric = registry.getMetric("test_histogram").?;
    try std.testing.expectEqual(@as(u64, 3), hist_metric.value.histogram.count);
    try std.testing.expectEqual(@as(f64, 4.5), hist_metric.value.histogram.sum);
}

test "metrics timer operations" {
    const allocator = std.testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    // Register timer
    try registry.register("test_timer", "A test timer", .timer);

    // Record some times
    try registry.recordTimer("test_timer", 1000000); // 1ms
    try registry.recordTimer("test_timer", 2000000); // 2ms
    try registry.recordTimer("test_timer", 3000000); // 3ms

    const timer_metric = registry.getMetric("test_timer").?;
    try std.testing.expectEqual(@as(u64, 3), timer_metric.value.timer.count);
    try std.testing.expectEqual(@as(u64, 6000000), timer_metric.value.timer.sum_ns);
    try std.testing.expectEqual(@as(f64, 2.0), timer_metric.value.timer.averageMs());
}

test "timer helper" {
    const allocator = std.testing.allocator;

    var registry = MetricsRegistry.init(allocator);
    defer registry.deinit();

    try registry.register("test_operation", "A test operation timer", .timer);

    const timer = Timer.start();

    // Simulate some work
    std.Thread.sleep(1000000); // 1ms

    try timer.record(&registry, "test_operation");

    const metric = registry.getMetric("test_operation").?;
    try std.testing.expectEqual(@as(u64, 1), metric.value.timer.count);
    try std.testing.expect(metric.value.timer.sum_ns >= 1000000); // At least 1ms
}

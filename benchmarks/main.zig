const std = @import("std");

// Import benchmark modules
const MessageBenchmarks = @import("message_benchmarks.zig");
const QueueBenchmarks = @import("queue_benchmarks.zig");
const ExchangeBenchmarks = @import("exchange_benchmarks.zig");
const RoutingBenchmarks = @import("routing_benchmarks.zig");
const StorageBenchmarks = @import("storage_benchmarks.zig");
const ConcurrencyBenchmarks = @import("concurrency_benchmarks.zig");

const BenchmarkResult = struct {
    name: []const u8,
    iterations: u64,
    total_time_ns: u64,
    avg_time_ns: u64,
    throughput_ops_per_sec: f64,
    memory_used_bytes: u64,
};

const BenchmarkSuite = struct {
    allocator: std.mem.Allocator,
    results: std.ArrayList(BenchmarkResult),

    pub fn init(allocator: std.mem.Allocator) BenchmarkSuite {
        return BenchmarkSuite{
            .allocator = allocator,
            .results = std.ArrayList(BenchmarkResult).init(allocator),
        };
    }

    pub fn deinit(self: *BenchmarkSuite) void {
        self.results.deinit();
    }

    pub fn runBenchmark(self: *BenchmarkSuite, name: []const u8, iterations: u64, benchmark_fn: *const fn (allocator: std.mem.Allocator, iteration: u64) anyerror!void) !void {
        std.log.info("Running benchmark: {s} ({} iterations)", .{ name, iterations });

        // Track memory usage
        var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = gpa.deinit();
        const bench_allocator = gpa.allocator();

        const start_time = std.time.nanoTimestamp();

        // Run benchmark iterations
        for (0..iterations) |i| {
            try benchmark_fn(bench_allocator, i);
        }

        const end_time = std.time.nanoTimestamp();

        const total_time_ns = @as(u64, @intCast(end_time - start_time));
        const avg_time_ns = total_time_ns / iterations;
        const throughput = @as(f64, @floatFromInt(iterations)) / (@as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0);
        const memory_used: u64 = 0; // Memory tracking disabled for now

        const result = BenchmarkResult{
            .name = try self.allocator.dupe(u8, name),
            .iterations = iterations,
            .total_time_ns = total_time_ns,
            .avg_time_ns = avg_time_ns,
            .throughput_ops_per_sec = throughput,
            .memory_used_bytes = memory_used,
        };

        try self.results.append(result);

        std.log.info("  Total time: {d:.2}ms, Avg: {d:.2}μs, Throughput: {d:.0} ops/sec, Memory: {} bytes",
            .{ @as(f64, @floatFromInt(total_time_ns)) / 1_000_000.0, @as(f64, @floatFromInt(avg_time_ns)) / 1000.0, throughput, memory_used });
    }

    pub fn printSummary(self: *const BenchmarkSuite) void {
        std.log.info("\n=== Benchmark Summary ===", .{});
        std.log.info("| {s:<30} | {s:>12} | {s:>12} | {s:>15} | {s:>12} |", .{ "Benchmark", "Iterations", "Avg Time", "Throughput", "Memory" });
        std.log.info("|{s:-<32}|{s:-<14}|{s:-<14}|{s:-<17}|{s:-<14}|", .{ "", "", "", "", "" });

        for (self.results.items) |result| {
            const avg_time_us = @as(f64, @floatFromInt(result.avg_time_ns)) / 1000.0;
            const memory_mb = @as(f64, @floatFromInt(result.memory_used_bytes)) / (1024.0 * 1024.0);
            
            std.log.info("| {s:<30} | {d:>12} | {d:>9.2}μs | {d:>11.0} ops/s | {d:>9.2}MB |",
                .{ result.name, result.iterations, avg_time_us, result.throughput_ops_per_sec, memory_mb });
        }
        std.log.info("|{s:-<32}|{s:-<14}|{s:-<14}|{s:-<17}|{s:-<14}|", .{ "", "", "", "", "" });
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Starting Yak AMQP Broker Performance Benchmarks", .{});
    std.log.info("=====================================================", .{});

    var suite = BenchmarkSuite.init(allocator);
    defer suite.deinit();

    // Run all benchmark categories
    try runMessageBenchmarks(&suite);
    try runQueueBenchmarks(&suite);
    try runExchangeBenchmarks(&suite);
    try runRoutingBenchmarks(&suite);
    try runStorageBenchmarks(&suite);
    try runConcurrencyBenchmarks(&suite);

    // Print final summary
    suite.printSummary();

    std.log.info("\nBenchmarks completed successfully!", .{});
}

fn runMessageBenchmarks(suite: *BenchmarkSuite) !void {
    std.log.info("\n--- Message Benchmarks ---", .{});
    
    try suite.runBenchmark("Message Creation", 100_000, MessageBenchmarks.benchmarkMessageCreation);
    try suite.runBenchmark("Message Serialization", 50_000, MessageBenchmarks.benchmarkMessageSerialization);
    try suite.runBenchmark("Message Deserialization", 50_000, MessageBenchmarks.benchmarkMessageDeserialization);
    try suite.runBenchmark("Message Header Operations", 100_000, MessageBenchmarks.benchmarkMessageHeaders);
    try suite.runBenchmark("Message Clone", 50_000, MessageBenchmarks.benchmarkMessageClone);
    try suite.runBenchmark("Dead Letter Operations", 25_000, MessageBenchmarks.benchmarkDeadLetterOperations);
}

fn runQueueBenchmarks(suite: *BenchmarkSuite) !void {
    std.log.info("\n--- Queue Benchmarks ---", .{});
    
    try suite.runBenchmark("Queue Publish", 100_000, QueueBenchmarks.benchmarkQueuePublish);
    try suite.runBenchmark("Queue Consume", 100_000, QueueBenchmarks.benchmarkQueueConsume);
    try suite.runBenchmark("Queue Acknowledge", 100_000, QueueBenchmarks.benchmarkQueueAcknowledge);
    try suite.runBenchmark("Queue Reject", 50_000, QueueBenchmarks.benchmarkQueueReject);
    try suite.runBenchmark("Queue Purge", 1_000, QueueBenchmarks.benchmarkQueuePurge);
}

fn runExchangeBenchmarks(suite: *BenchmarkSuite) !void {
    std.log.info("\n--- Exchange Benchmarks ---", .{});
    
    try suite.runBenchmark("Direct Exchange Routing", 100_000, ExchangeBenchmarks.benchmarkDirectExchangeRouting);
    try suite.runBenchmark("Fanout Exchange Routing", 50_000, ExchangeBenchmarks.benchmarkFanoutExchangeRouting);
    try suite.runBenchmark("Topic Exchange Routing", 25_000, ExchangeBenchmarks.benchmarkTopicExchangeRouting);
    try suite.runBenchmark("Headers Exchange Routing", 10_000, ExchangeBenchmarks.benchmarkHeadersExchangeRouting);
}

fn runRoutingBenchmarks(suite: *BenchmarkSuite) !void {
    std.log.info("\n--- Routing Benchmarks ---", .{});
    
    try suite.runBenchmark("Message Routing E2E", 50_000, RoutingBenchmarks.benchmarkEndToEndRouting);
    try suite.runBenchmark("Binding Operations", 25_000, RoutingBenchmarks.benchmarkBindingOperations);
    try suite.runBenchmark("Topic Pattern Matching", 100_000, RoutingBenchmarks.benchmarkTopicPatternMatching);
}

fn runStorageBenchmarks(suite: *BenchmarkSuite) !void {
    std.log.info("\n--- Storage Benchmarks ---", .{});
    
    try suite.runBenchmark("Message Storage Write", 25_000, StorageBenchmarks.benchmarkMessageStorageWrite);
    try suite.runBenchmark("Message Storage Read", 25_000, StorageBenchmarks.benchmarkMessageStorageRead);
    try suite.runBenchmark("Message Storage Batch Write", 5_000, StorageBenchmarks.benchmarkMessageStorageBatchWrite);
    try suite.runBenchmark("Message Storage Batch Read", 5_000, StorageBenchmarks.benchmarkMessageStorageBatchRead);
}

fn runConcurrencyBenchmarks(suite: *BenchmarkSuite) !void {
    std.log.info("\n--- Concurrency Benchmarks ---", .{});
    
    try suite.runBenchmark("Concurrent Queue Access", 10_000, ConcurrencyBenchmarks.benchmarkConcurrentQueueAccess);
    try suite.runBenchmark("Concurrent Message Publish", 10_000, ConcurrencyBenchmarks.benchmarkConcurrentMessagePublish);
    try suite.runBenchmark("Concurrent Consumer Operations", 5_000, ConcurrencyBenchmarks.benchmarkConcurrentConsumerOperations);
}
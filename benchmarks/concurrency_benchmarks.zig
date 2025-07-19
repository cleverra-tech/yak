const std = @import("std");
const Queue = @import("yak").queue.Queue;
const Message = @import("yak").message.Message;
const Consumer = @import("yak").consumer.Consumer;

const ThreadContext = struct {
    allocator: std.mem.Allocator,
    queue: *Queue,
    iterations: u64,
    thread_id: u64,
};

fn concurrentPublishWorker(context: *ThreadContext) !void {
    for (0..context.iterations) |i| {
        const msg_id = context.thread_id * context.iterations + i;
        var message = try Message.init(context.allocator, msg_id, "test.exchange", "test.key", "Concurrent publish message");
        defer message.deinit();

        try context.queue.publish(message);
    }
}

fn concurrentConsumeWorker(context: *ThreadContext) !void {
    for (0..context.iterations) |_| {
        if (try context.queue.get(false)) |message| {
            var msg = message;
            defer msg.deinit();

            // Simulate some processing
            std.Thread.sleep(1000); // 1 microsecond
        }
    }
}

pub fn benchmarkConcurrentQueueAccess(allocator: std.mem.Allocator, iteration: u64) !void {
    _ = iteration;

    var queue = try Queue.init(allocator, "concurrent.queue", true, false, false, null);
    defer queue.deinit();

    const thread_count = 4;
    const per_thread_ops = 100;

    var threads: [thread_count]std.Thread = undefined;
    var contexts: [thread_count]ThreadContext = undefined;

    // Initialize contexts
    for (0..thread_count) |i| {
        contexts[i] = ThreadContext{
            .allocator = allocator,
            .queue = &queue,
            .iterations = per_thread_ops,
            .thread_id = i,
        };
    }

    // Start publisher threads
    for (0..thread_count) |i| {
        threads[i] = try std.Thread.spawn(.{}, concurrentPublishWorker, .{&contexts[i]});
    }

    // Wait for all threads to complete
    for (0..thread_count) |i| {
        threads[i].join();
    }
}

pub fn benchmarkConcurrentMessagePublish(allocator: std.mem.Allocator, iteration: u64) !void {
    _ = iteration;

    var queue = try Queue.init(allocator, "publish.queue", true, false, false, null);
    defer queue.deinit();

    const thread_count = 8;
    const per_thread_ops = 50;

    var threads: [thread_count]std.Thread = undefined;
    var contexts: [thread_count]ThreadContext = undefined;

    // Initialize contexts
    for (0..thread_count) |i| {
        contexts[i] = ThreadContext{
            .allocator = allocator,
            .queue = &queue,
            .iterations = per_thread_ops,
            .thread_id = i,
        };
    }

    // Start threads
    for (0..thread_count) |i| {
        threads[i] = try std.Thread.spawn(.{}, concurrentPublishWorker, .{&contexts[i]});
    }

    // Wait for completion
    for (0..thread_count) |i| {
        threads[i].join();
    }
}

pub fn benchmarkConcurrentConsumerOperations(allocator: std.mem.Allocator, iteration: u64) !void {
    _ = iteration;

    var queue = try Queue.init(allocator, "consumer.queue", true, false, false, null);
    defer queue.deinit();

    // Pre-populate queue with messages
    for (0..1000) |i| {
        const message = try Message.init(allocator, i, "test.exchange", "test.key", "Consumer test message");
        try queue.publish(message);
    }

    const thread_count = 4;
    const per_thread_ops = 25;

    var threads: [thread_count]std.Thread = undefined;
    var contexts: [thread_count]ThreadContext = undefined;

    // Initialize contexts
    for (0..thread_count) |i| {
        contexts[i] = ThreadContext{
            .allocator = allocator,
            .queue = &queue,
            .iterations = per_thread_ops,
            .thread_id = i,
        };
    }

    // Start consumer threads
    for (0..thread_count) |i| {
        threads[i] = try std.Thread.spawn(.{}, concurrentConsumeWorker, .{&contexts[i]});
    }

    // Wait for completion
    for (0..thread_count) |i| {
        threads[i].join();
    }
}

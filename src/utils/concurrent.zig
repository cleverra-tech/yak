const std = @import("std");

/// A thread-safe queue for passing data between threads
pub fn ThreadSafeQueue(comptime T: type) type {
    return struct {
        const Self = @This();

        items: std.ArrayList(T),
        mutex: std.Thread.Mutex,
        condition: std.Thread.Condition,
        closed: bool,

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .items = std.ArrayList(T).init(allocator),
                .mutex = std.Thread.Mutex{},
                .condition = std.Thread.Condition{},
                .closed = false,
            };
        }

        pub fn deinit(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.items.deinit();
        }

        pub fn push(self: *Self, item: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) {
                return error.QueueClosed;
            }

            try self.items.append(item);
            self.condition.signal();
        }

        pub fn pop(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.items.items.len > 0) {
                return self.items.orderedRemove(0);
            }

            return null;
        }

        pub fn popWait(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.items.items.len == 0 and !self.closed) {
                self.condition.wait(&self.mutex);
            }

            if (self.items.items.len > 0) {
                return self.items.orderedRemove(0);
            }

            return null;
        }

        pub fn close(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.closed = true;
            self.condition.broadcast();
        }

        pub fn len(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.items.items.len;
        }

        pub fn isClosed(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            return self.closed;
        }
    };
}

/// A worker thread pool for handling tasks
pub const ThreadPool = struct {
    threads: std.ArrayList(std.Thread),
    work_queue: ThreadSafeQueue(WorkItem),
    running: bool,
    allocator: std.mem.Allocator,

    const WorkItem = struct {
        func: *const fn (data: ?*anyopaque) void,
        data: ?*anyopaque,
    };

    pub fn init(allocator: std.mem.Allocator, thread_count: usize) !ThreadPool {
        var pool = ThreadPool{
            .threads = std.ArrayList(std.Thread).init(allocator),
            .work_queue = ThreadSafeQueue(WorkItem).init(allocator),
            .running = true,
            .allocator = allocator,
        };

        // Start worker threads
        for (0..thread_count) |_| {
            const thread = try std.Thread.spawn(.{}, workerThread, .{&pool});
            try pool.threads.append(thread);
        }

        return pool;
    }

    pub fn deinit(self: *ThreadPool) void {
        self.shutdown();
        self.work_queue.deinit();
        self.threads.deinit();
    }

    pub fn submit(self: *ThreadPool, func: *const fn (data: ?*anyopaque) void, data: ?*anyopaque) !void {
        if (!self.running) {
            return error.ThreadPoolShutdown;
        }

        const work_item = WorkItem{
            .func = func,
            .data = data,
        };

        try self.work_queue.push(work_item);
    }

    pub fn shutdown(self: *ThreadPool) void {
        self.running = false;
        self.work_queue.close();

        // Wait for all threads to complete
        for (self.threads.items) |thread| {
            thread.join();
        }
    }

    fn workerThread(pool: *ThreadPool) void {
        while (pool.running) {
            if (pool.work_queue.popWait()) |work_item| {
                work_item.func(work_item.data);
            }
        }
    }

    pub fn getActiveThreadCount(self: *const ThreadPool) usize {
        return self.threads.items.len;
    }

    pub fn getQueueLength(self: *ThreadPool) usize {
        return self.work_queue.len();
    }
};

/// An atomic counter for statistics
pub const AtomicCounter = struct {
    value: u64,

    pub fn init(initial_value: u64) AtomicCounter {
        return AtomicCounter{
            .value = initial_value,
        };
    }

    pub fn increment(self: *AtomicCounter) u64 {
        return @atomicRmw(u64, &self.value, .Add, 1, .seq_cst);
    }

    pub fn decrement(self: *AtomicCounter) u64 {
        return @atomicRmw(u64, &self.value, .Sub, 1, .seq_cst);
    }

    pub fn add(self: *AtomicCounter, delta: u64) u64 {
        return @atomicRmw(u64, &self.value, .Add, delta, .seq_cst);
    }

    pub fn get(self: *const AtomicCounter) u64 {
        return @atomicLoad(u64, &self.value, .seq_cst);
    }

    pub fn set(self: *AtomicCounter, new_value: u64) void {
        @atomicStore(u64, &self.value, new_value, .seq_cst);
    }

    pub fn compareAndSwap(self: *AtomicCounter, expected: u64, new_value: u64) bool {
        const result = @cmpxchgWeak(u64, &self.value, expected, new_value, .seq_cst, .seq_cst);
        return result == null;
    }
};

/// A read-write lock for concurrent access
pub const RwLock = struct {
    mutex: std.Thread.Mutex,
    read_condition: std.Thread.Condition,
    write_condition: std.Thread.Condition,
    readers: u32,
    writers: u32,
    write_requests: u32,

    pub fn init() RwLock {
        return RwLock{
            .mutex = std.Thread.Mutex{},
            .read_condition = std.Thread.Condition{},
            .write_condition = std.Thread.Condition{},
            .readers = 0,
            .writers = 0,
            .write_requests = 0,
        };
    }

    pub fn readLock(self: *RwLock) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.writers > 0 or self.write_requests > 0) {
            self.read_condition.wait(&self.mutex);
        }

        self.readers += 1;
    }

    pub fn readUnlock(self: *RwLock) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.readers -= 1;

        if (self.readers == 0) {
            self.write_condition.signal();
        }
    }

    pub fn writeLock(self: *RwLock) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.write_requests += 1;

        while (self.writers > 0 or self.readers > 0) {
            self.write_condition.wait(&self.mutex);
        }

        self.write_requests -= 1;
        self.writers += 1;
    }

    pub fn writeUnlock(self: *RwLock) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        self.writers -= 1;

        if (self.write_requests > 0) {
            self.write_condition.signal();
        } else {
            self.read_condition.broadcast();
        }
    }
};

/// Utilities for concurrent programming
pub const ConcurrentUtils = struct {
    pub fn spinWait(cycles: u64) void {
        var i: u64 = 0;
        while (i < cycles) : (i += 1) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn getCpuCount() u32 {
        return @intCast(std.Thread.getCpuCount() catch 1);
    }

    pub fn getCurrentThreadId() std.Thread.Id {
        return std.Thread.getCurrentId();
    }
};

test "thread safe queue operations" {
    const allocator = std.testing.allocator;

    var queue = ThreadSafeQueue(u32).init(allocator);
    defer queue.deinit();

    try std.testing.expectEqual(@as(usize, 0), queue.len());
    try std.testing.expectEqual(false, queue.isClosed());

    // Push some items
    try queue.push(1);
    try queue.push(2);
    try queue.push(3);

    try std.testing.expectEqual(@as(usize, 3), queue.len());

    // Pop items
    try std.testing.expectEqual(@as(u32, 1), queue.pop().?);
    try std.testing.expectEqual(@as(u32, 2), queue.pop().?);
    try std.testing.expectEqual(@as(usize, 1), queue.len());

    // Close the queue
    queue.close();
    try std.testing.expectEqual(true, queue.isClosed());

    // Should not be able to push to closed queue
    try std.testing.expectError(error.QueueClosed, queue.push(4));
}

test "atomic counter operations" {
    var counter = AtomicCounter.init(0);

    try std.testing.expectEqual(@as(u64, 0), counter.get());

    const prev1 = counter.increment();
    try std.testing.expectEqual(@as(u64, 0), prev1);
    try std.testing.expectEqual(@as(u64, 1), counter.get());

    const prev2 = counter.add(5);
    try std.testing.expectEqual(@as(u64, 1), prev2);
    try std.testing.expectEqual(@as(u64, 6), counter.get());

    const prev3 = counter.decrement();
    try std.testing.expectEqual(@as(u64, 6), prev3);
    try std.testing.expectEqual(@as(u64, 5), counter.get());

    // Test compare and swap
    try std.testing.expectEqual(true, counter.compareAndSwap(5, 10));
    try std.testing.expectEqual(@as(u64, 10), counter.get());

    try std.testing.expectEqual(false, counter.compareAndSwap(5, 15));
    try std.testing.expectEqual(@as(u64, 10), counter.get());
}

test "thread pool basic operations" {
    const allocator = std.testing.allocator;

    var pool = try ThreadPool.init(allocator, 2);
    defer pool.deinit();

    try std.testing.expectEqual(@as(usize, 2), pool.getActiveThreadCount());
    try std.testing.expectEqual(@as(usize, 0), pool.getQueueLength());

    // Simple test function
    const TestData = struct {
        var counter: u32 = 0;
        var mutex: std.Thread.Mutex = std.Thread.Mutex{};

        fn workFunc(data: ?*anyopaque) void {
            _ = data;
            mutex.lock();
            defer mutex.unlock();
            counter += 1;
        }
    };

    // Submit some work
    try pool.submit(TestData.workFunc, null);
    try pool.submit(TestData.workFunc, null);

    // Give threads time to process
    std.time.sleep(100 * std.time.ns_per_ms);

    TestData.mutex.lock();
    const final_counter = TestData.counter;
    TestData.mutex.unlock();

    try std.testing.expectEqual(@as(u32, 2), final_counter);
}

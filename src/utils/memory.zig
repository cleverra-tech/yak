const std = @import("std");

/// A memory pool for reducing allocations in high-frequency operations
pub const MemoryPool = struct {
    blocks: std.ArrayList([]u8),
    block_size: usize,
    free_blocks: std.ArrayList([]u8),
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, block_size: usize, initial_blocks: usize) !MemoryPool {
        var pool = MemoryPool{
            .blocks = std.ArrayList([]u8).init(allocator),
            .block_size = block_size,
            .free_blocks = std.ArrayList([]u8).init(allocator),
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };

        // Pre-allocate initial blocks
        for (0..initial_blocks) |_| {
            const block = try allocator.alloc(u8, block_size);
            try pool.blocks.append(block);
            try pool.free_blocks.append(block);
        }

        return pool;
    }

    pub fn deinit(self: *MemoryPool) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.blocks.items) |block| {
            self.allocator.free(block);
        }
        self.blocks.deinit();
        self.free_blocks.deinit();
    }

    pub fn acquire(self: *MemoryPool) ![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.free_blocks.items.len > 0) {
            return self.free_blocks.pop().?;
        }

        // Allocate new block if pool is empty
        const block = try self.allocator.alloc(u8, self.block_size);
        try self.blocks.append(block);
        return block;
    }

    pub fn release(self: *MemoryPool, block: []u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Clear the block before returning to pool
        @memset(block, 0);
        self.free_blocks.append(block) catch {
            // If we can't add back to pool, just leave it allocated
            // It will be cleaned up in deinit()
        };
    }

    pub fn getStats(self: *const MemoryPool) struct { total: usize, free: usize, used: usize } {
        return .{
            .total = self.blocks.items.len,
            .free = self.free_blocks.items.len,
            .used = self.blocks.items.len - self.free_blocks.items.len,
        };
    }
};

/// A ring buffer for efficient message queuing
pub const RingBuffer = struct {
    buffer: []u8,
    head: usize,
    tail: usize,
    size: usize,
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, buffer_capacity: usize) !RingBuffer {
        const buffer = try allocator.alloc(u8, buffer_capacity);
        return RingBuffer{
            .buffer = buffer,
            .head = 0,
            .tail = 0,
            .size = 0,
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *RingBuffer) void {
        self.allocator.free(self.buffer);
    }

    pub fn write(self: *RingBuffer, data: []const u8) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const space_available = self.buffer.len - self.size;
        const to_write = @min(data.len, space_available);

        if (to_write == 0) {
            return 0;
        }

        var written: usize = 0;
        while (written < to_write) {
            const chunk_size = @min(to_write - written, self.buffer.len - self.tail);
            @memcpy(self.buffer[self.tail .. self.tail + chunk_size], data[written .. written + chunk_size]);

            self.tail = (self.tail + chunk_size) % self.buffer.len;
            written += chunk_size;
        }

        self.size += to_write;
        return to_write;
    }

    pub fn read(self: *RingBuffer, dest: []u8) usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        const to_read = @min(dest.len, self.size);

        if (to_read == 0) {
            return 0;
        }

        var read_count: usize = 0;
        while (read_count < to_read) {
            const chunk_size = @min(to_read - read_count, self.buffer.len - self.head);
            @memcpy(dest[read_count .. read_count + chunk_size], self.buffer[self.head .. self.head + chunk_size]);

            self.head = (self.head + chunk_size) % self.buffer.len;
            read_count += chunk_size;
        }

        self.size -= to_read;
        return to_read;
    }

    pub fn available(self: *const RingBuffer) usize {
        return self.size;
    }

    pub fn capacity(self: *const RingBuffer) usize {
        return self.buffer.len;
    }

    pub fn isFull(self: *const RingBuffer) bool {
        return self.size == self.buffer.len;
    }

    pub fn isEmpty(self: *const RingBuffer) bool {
        return self.size == 0;
    }
};

/// A memory-mapped file for efficient persistence
pub const MemoryMappedFile = struct {
    file: std.fs.File,
    mapping: []align(std.mem.page_size) u8,
    size: usize,

    pub fn init(file_path: []const u8, size: usize) !MemoryMappedFile {
        const file = try std.fs.cwd().createFile(file_path, .{ .read = true, .truncate = false });

        // Ensure file is at least the requested size
        const current_size = try file.getEndPos();
        if (current_size < size) {
            try file.setEndPos(size);
        }

        const mapping = try std.posix.mmap(
            null,
            size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        return MemoryMappedFile{
            .file = file,
            .mapping = mapping,
            .size = size,
        };
    }

    pub fn deinit(self: *MemoryMappedFile) void {
        std.posix.munmap(self.mapping);
        self.file.close();
    }

    pub fn sync(self: *MemoryMappedFile) !void {
        try std.posix.msync(self.mapping, .{ .ASYNC = false });
    }

    pub fn getData(self: *const MemoryMappedFile) []u8 {
        return self.mapping;
    }
};

/// Utilities for memory management and debugging
pub const MemoryUtils = struct {
    pub fn getProcessMemoryUsage() !usize {
        // Platform-specific memory usage detection
        switch (@import("builtin").os.tag) {
            .linux => {
                return getLinuxMemoryUsage() catch 0;
            },
            .macos => {
                return getMacOSMemoryUsage() catch 0;
            },
            .windows => {
                return getWindowsMemoryUsage() catch 0;
            },
            .freebsd, .netbsd, .openbsd => {
                return getBSDMemoryUsage() catch 0;
            },
            else => {
                // Fallback: Use a basic heap estimation
                return getFallbackMemoryUsage();
            },
        }
    }

    fn getLinuxMemoryUsage() !usize {
        const file = try std.fs.cwd().openFile("/proc/self/status", .{});
        defer file.close();

        var buf: [8192]u8 = undefined;
        const bytes_read = try file.readAll(&buf);
        const contents = buf[0..bytes_read];

        var lines = std.mem.splitSequence(u8, contents, "\n");
        while (lines.next()) |line| {
            if (std.mem.startsWith(u8, line, "VmRSS:")) {
                var parts = std.mem.tokenizeAny(u8, line, " \t");
                _ = parts.next(); // Skip "VmRSS:"
                if (parts.next()) |size_str| {
                    const size_kb = try std.fmt.parseInt(usize, size_str, 10);
                    return size_kb * 1024; // Convert KB to bytes
                }
            }
        }
        return error.MemoryInfoNotFound;
    }

    fn getMacOSMemoryUsage() !usize {
        // Try to read from system files or use ps command
        return getPSMemoryUsage() catch getFallbackMemoryUsage();
    }

    fn getWindowsMemoryUsage() !usize {
        // Use fixed buffer for pid filter instead of allocation
        var pid_filter_buf: [64]u8 = undefined;
        const pid_filter = try std.fmt.bufPrint(&pid_filter_buf, "pid eq {}", .{std.process.getpid()});

        // Use a small arena for the subprocess call result
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const temp_allocator = arena.allocator();

        const result = std.process.Child.run(.{
            .allocator = temp_allocator,
            .argv = &[_][]const u8{ "tasklist", "/fi", pid_filter, "/fo", "csv" },
        }) catch return error.MemoryInfoNotFound;

        // Parse CSV output to extract memory usage
        var lines = std.mem.splitSequence(u8, result.stdout, "\n");
        var line_count: u32 = 0;
        while (lines.next()) |line| {
            line_count += 1;
            if (line_count == 2) { // Skip header, process second line
                var fields = std.mem.splitSequence(u8, line, ",");
                var field_count: u32 = 0;
                while (fields.next()) |field| {
                    field_count += 1;
                    if (field_count == 5) { // Memory usage is typically 5th field
                        const clean_field = std.mem.trim(u8, field, "\" ,K");
                        const mem_kb = std.fmt.parseInt(usize, clean_field, 10) catch continue;
                        return mem_kb * 1024;
                    }
                }
                break;
            }
        }

        return error.MemoryInfoNotFound;
    }

    fn getBSDMemoryUsage() !usize {
        // Use ps command for BSD systems
        return getPSMemoryUsage() catch getFallbackMemoryUsage();
    }

    fn getPSMemoryUsage() !usize {
        // Use fixed buffer for pid string instead of allocation
        var pid_buf: [32]u8 = undefined;
        const pid_str = try std.fmt.bufPrint(&pid_buf, "{}", .{std.process.getpid()});

        // Use a small arena for the subprocess call result
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const temp_allocator = arena.allocator();

        const result = std.process.Child.run(.{
            .allocator = temp_allocator,
            .argv = &[_][]const u8{ "ps", "-o", "rss=", "-p", pid_str },
        }) catch return error.MemoryInfoNotFound;

        const rss_str = std.mem.trim(u8, result.stdout, " \t\n\r");
        const rss_kb = std.fmt.parseInt(usize, rss_str, 10) catch return error.MemoryInfoNotFound;

        return rss_kb * 1024; // Convert KB to bytes
    }

    fn getFallbackMemoryUsage() usize {
        // Fallback method: estimate based on heap allocations
        // This is not accurate but provides a basic indication
        const heap_stats = std.heap.page_allocator.vtable;
        _ = heap_stats; // Suppress unused variable warning

        // Return a conservative estimate
        // In practice, this should be enhanced with more sophisticated tracking
        return 1024 * 1024; // 1MB base estimate
    }

    pub fn formatBytes(bytes: usize, allocator: std.mem.Allocator) ![]u8 {
        const units = [_][]const u8{ "B", "KB", "MB", "GB", "TB" };
        var size = @as(f64, @floatFromInt(bytes));
        var unit_index: usize = 0;

        while (size >= 1024.0 and unit_index < units.len - 1) {
            size /= 1024.0;
            unit_index += 1;
        }

        if (unit_index == 0) {
            return try std.fmt.allocPrint(allocator, "{d} {s}", .{ @as(usize, @intFromFloat(size)), units[unit_index] });
        } else {
            return try std.fmt.allocPrint(allocator, "{d:.2} {s}", .{ size, units[unit_index] });
        }
    }

    pub fn alignSize(size: usize, alignment: usize) usize {
        return (size + alignment - 1) & ~(alignment - 1);
    }
};

test "memory pool operations" {
    const allocator = std.testing.allocator;

    var pool = try MemoryPool.init(allocator, 1024, 2);
    defer pool.deinit();

    const stats = pool.getStats();
    try std.testing.expectEqual(@as(usize, 2), stats.total);
    try std.testing.expectEqual(@as(usize, 2), stats.free);
    try std.testing.expectEqual(@as(usize, 0), stats.used);

    // Acquire a block
    const block1 = try pool.acquire();
    try std.testing.expectEqual(@as(usize, 1024), block1.len);

    const stats_after_acquire = pool.getStats();
    try std.testing.expectEqual(@as(usize, 2), stats_after_acquire.total);
    try std.testing.expectEqual(@as(usize, 1), stats_after_acquire.free);
    try std.testing.expectEqual(@as(usize, 1), stats_after_acquire.used);

    // Release the block
    pool.release(block1);

    const stats_after_release = pool.getStats();
    try std.testing.expectEqual(@as(usize, 2), stats_after_release.total);
    try std.testing.expectEqual(@as(usize, 2), stats_after_release.free);
    try std.testing.expectEqual(@as(usize, 0), stats_after_release.used);
}

test "ring buffer operations" {
    const allocator = std.testing.allocator;

    var ring = try RingBuffer.init(allocator, 10);
    defer ring.deinit();

    try std.testing.expectEqual(@as(usize, 10), ring.capacity());
    try std.testing.expectEqual(@as(usize, 0), ring.available());
    try std.testing.expectEqual(true, ring.isEmpty());
    try std.testing.expectEqual(false, ring.isFull());

    // Write some data
    const data = "hello";
    const written = try ring.write(data);
    try std.testing.expectEqual(@as(usize, 5), written);
    try std.testing.expectEqual(@as(usize, 5), ring.available());

    // Read the data back
    var buffer: [10]u8 = undefined;
    const read_count = ring.read(&buffer);
    try std.testing.expectEqual(@as(usize, 5), read_count);
    try std.testing.expectEqualStrings("hello", buffer[0..read_count]);

    try std.testing.expectEqual(@as(usize, 0), ring.available());
    try std.testing.expectEqual(true, ring.isEmpty());
}

test "memory utils" {
    const allocator = std.testing.allocator;

    // Test byte formatting
    const formatted_bytes = try MemoryUtils.formatBytes(1536, allocator);
    defer allocator.free(formatted_bytes);
    try std.testing.expectEqualStrings("1.50 KB", formatted_bytes);

    const formatted_gb = try MemoryUtils.formatBytes(1073741824, allocator);
    defer allocator.free(formatted_gb);
    try std.testing.expectEqualStrings("1.00 GB", formatted_gb);

    // Test alignment
    try std.testing.expectEqual(@as(usize, 8), MemoryUtils.alignSize(5, 8));
    try std.testing.expectEqual(@as(usize, 16), MemoryUtils.alignSize(15, 8));
    try std.testing.expectEqual(@as(usize, 16), MemoryUtils.alignSize(16, 8));
}

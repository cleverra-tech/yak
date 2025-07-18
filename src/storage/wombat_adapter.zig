const std = @import("std");
const wombat = @import("wombat");
const Config = @import("../config.zig").Config;

pub const WombatAdapter = struct {
    db: *wombat.DB,
    allocator: std.mem.Allocator,
    options: wombat.Options,

    pub fn init(allocator: std.mem.Allocator, config: *const Config.WombatStorageConfig) !WombatAdapter {
        // Convert Yak config to Wombat options
        const options = wombat.Options{
            .dir = config.data_dir,
            .value_dir = config.value_dir,
            .mem_table_size = config.mem_table_size,
            .num_memtables = config.num_memtables,
            .max_levels = config.max_levels,
            .num_compactors = config.num_compactors,
            .value_threshold = config.value_threshold,
            .value_log_file_size = config.value_log_file_size,
            .compression = if (config.sync_writes) .snappy else .none,
            .sync_writes = config.sync_writes,
            .block_size = config.block_size,
            .bloom_false_positive = config.bloom_false_positive,
            .level_size_multiplier = config.level_size_multiplier,
            .base_table_size = config.base_table_size,
            .num_level_zero_tables = config.num_level_zero_tables,
            .num_level_zero_tables_stall = config.num_level_zero_tables_stall,
            .detect_conflicts = config.detect_conflicts,
            .verify_checksums = config.verify_checksums,
        };

        // Create data directories if they don't exist
        try std.fs.cwd().makePath(config.data_dir);
        try std.fs.cwd().makePath(config.value_dir);

        // Open Wombat database
        const db = try wombat.DB.open(allocator, options);

        return WombatAdapter{
            .db = db,
            .allocator = allocator,
            .options = options,
        };
    }

    pub fn deinit(self: *WombatAdapter) void {
        self.db.close();
    }

    pub fn set(self: *WombatAdapter, key: []const u8, value: []const u8) !void {
        var txn = try self.db.newTransaction(.{ .update = true });
        defer self.db.discardTransaction(txn);

        try txn.set(key, value);
        try self.db.commitTransaction(txn);
    }

    pub fn get(self: *WombatAdapter, key: []const u8) !?[]u8 {
        var txn = try self.db.newTransaction(.{ .update = false });
        defer self.db.discardTransaction(txn);

        return try txn.get(self.allocator, key);
    }

    pub fn delete(self: *WombatAdapter, key: []const u8) !void {
        var txn = try self.db.newTransaction(.{ .update = true });
        defer self.db.discardTransaction(txn);

        try txn.delete(key);
        try self.db.commitTransaction(txn);
    }

    pub fn newTransaction(self: *WombatAdapter, update: bool) !*wombat.Txn {
        return try self.db.newTransaction(.{ .update = update });
    }

    pub fn commitTransaction(self: *WombatAdapter, txn: *wombat.Txn) !void {
        try self.db.commitTransaction(txn);
    }

    pub fn discardTransaction(self: *WombatAdapter, txn: *wombat.Txn) void {
        self.db.discardTransaction(txn);
    }

    pub fn sync(self: *WombatAdapter) !void {
        try self.db.sync();
    }

    pub fn getStats(self: *WombatAdapter) wombat.DBStats {
        return self.db.getStats();
    }

    // Batch operations for high-performance scenarios
    pub fn setBatch(self: *WombatAdapter, entries: []const KVPair) !void {
        var txn = try self.db.newTransaction(.{ .update = true });
        defer self.db.discardTransaction(txn);

        for (entries) |entry| {
            try txn.set(entry.key, entry.value);
        }

        try self.db.commitTransaction(txn);
    }

    pub fn getBatch(self: *WombatAdapter, keys: []const []const u8) !std.ArrayList(?[]u8) {
        var txn = try self.db.newTransaction(.{ .update = false });
        defer self.db.discardTransaction(txn);

        var results = std.ArrayList(?[]u8).init(self.allocator);
        errdefer results.deinit();

        for (keys) |key| {
            const value = try txn.get(self.allocator, key);
            try results.append(value);
        }

        return results;
    }

    pub fn deleteBatch(self: *WombatAdapter, keys: []const []const u8) !void {
        var txn = try self.db.newTransaction(.{ .update = true });
        defer self.db.discardTransaction(txn);

        for (keys) |key| {
            try txn.delete(key);
        }

        try self.db.commitTransaction(txn);
    }

    // Iterator support for scanning
    pub fn scan(self: *WombatAdapter, prefix: []const u8, allocator: std.mem.Allocator) !std.ArrayList(KVPair) {
        var txn = try self.db.newTransaction(.{ .update = false });
        defer self.db.discardTransaction(txn);

        var results = std.ArrayList(KVPair).init(allocator);
        errdefer results.deinit();

        var iterator = try txn.newIterator(.{});
        defer iterator.close();

        try iterator.seek(prefix);

        while (iterator.valid()) {
            const key = try iterator.key(allocator);
            errdefer allocator.free(key);

            // Check if key still matches prefix
            if (!std.mem.startsWith(u8, key, prefix)) {
                allocator.free(key);
                break;
            }

            const value = try iterator.value(allocator);
            errdefer allocator.free(value);

            try results.append(KVPair{
                .key = key,
                .value = value,
            });

            try iterator.next();
        }

        return results;
    }

    // Storage key utilities for Yak-specific patterns
    pub fn generateMessageKey(allocator: std.mem.Allocator, queue_name: []const u8, message_id: u64) ![]u8 {
        return try std.fmt.allocPrint(allocator, "queue:{s}:msg:{d}", .{ queue_name, message_id });
    }

    pub fn generateQueueMetadataKey(allocator: std.mem.Allocator, queue_name: []const u8) ![]u8 {
        return try std.fmt.allocPrint(allocator, "queue:{s}:metadata", .{queue_name});
    }

    pub fn generateExchangeMetadataKey(allocator: std.mem.Allocator, exchange_name: []const u8) ![]u8 {
        return try std.fmt.allocPrint(allocator, "exchange:{s}:metadata", .{exchange_name});
    }

    pub fn generateBindingKey(allocator: std.mem.Allocator, exchange_name: []const u8, queue_name: []const u8, routing_key: []const u8) ![]u8 {
        return try std.fmt.allocPrint(allocator, "binding:{s}:{s}:{s}", .{ exchange_name, queue_name, routing_key });
    }

    pub fn generateServerConfigKey(allocator: std.mem.Allocator) ![]u8 {
        return try allocator.dupe(u8, "server:config");
    }

    pub fn generateServerStatsKey(allocator: std.mem.Allocator) ![]u8 {
        return try allocator.dupe(u8, "server:stats");
    }

    pub fn generateUsersKey(allocator: std.mem.Allocator) ![]u8 {
        return try allocator.dupe(u8, "server:users");
    }

    // Backup and recovery
    pub fn backup(self: *WombatAdapter, backup_dir: []const u8) !void {
        try self.db.backup(backup_dir);
        std.log.info("Database backup completed to: {s}", .{backup_dir});
    }

    pub fn restore(self: *WombatAdapter, backup_dir: []const u8) !void {
        try self.db.restore(backup_dir);
        std.log.info("Database restored from: {s}", .{backup_dir});
    }

    // Compaction control
    pub fn runCompaction(self: *WombatAdapter) !void {
        try self.db.runCompaction();
        std.log.info("Manual compaction completed");
    }

    pub fn getSize(self: *WombatAdapter) u64 {
        return self.db.getSize();
    }

    pub fn getMemoryUsage(self: *WombatAdapter) u64 {
        return self.db.getMemoryUsage();
    }

    // Health check
    pub fn healthCheck(self: *WombatAdapter) !bool {
        // Perform a simple read/write test
        const test_key = "health_check";
        const test_value = "ok";

        try self.set(test_key, test_value);

        if (try self.get(test_key)) |value| {
            defer self.allocator.free(value);
            if (std.mem.eql(u8, value, test_value)) {
                try self.delete(test_key);
                return true;
            }
        }

        return false;
    }
};

pub const KVPair = struct {
    key: []const u8,
    value: []const u8,

    pub fn deinit(self: *const KVPair, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }
};

// Wombat DB wrapper to match expected interface
pub const DB = struct {
    adapter: WombatAdapter,

    pub fn open(allocator: std.mem.Allocator, config: *const Config.WombatStorageConfig) !*DB {
        const db = try allocator.create(DB);
        db.adapter = try WombatAdapter.init(allocator, config);
        return db;
    }

    pub fn close(self: *DB) void {
        self.adapter.deinit();
        self.adapter.allocator.destroy(self);
    }

    pub fn set(self: *DB, key: []const u8, value: []const u8) !void {
        return self.adapter.set(key, value);
    }

    pub fn get(self: *DB, key: []const u8) !?[]u8 {
        return self.adapter.get(key);
    }

    pub fn delete(self: *DB, key: []const u8) !void {
        return self.adapter.delete(key);
    }

    pub fn newTransaction(self: *DB) !*wombat.Txn {
        return self.adapter.newTransaction(true);
    }

    pub fn commitTransaction(self: *DB, txn: *wombat.Txn) !void {
        return self.adapter.commitTransaction(txn);
    }

    pub fn discardTransaction(self: *DB, txn: *wombat.Txn) void {
        return self.adapter.discardTransaction(txn);
    }

    pub fn getStats(self: *DB) wombat.DBStats {
        return self.adapter.getStats();
    }
};

test "wombat adapter basic operations" {
    const allocator = std.testing.allocator;

    // Create temporary directory for test
    const test_dir = "test_wombat_data";
    const test_value_dir = "test_wombat_values";

    std.fs.cwd().deleteTree(test_dir) catch {};
    std.fs.cwd().deleteTree(test_value_dir) catch {};
    defer {
        std.fs.cwd().deleteTree(test_dir) catch {};
        std.fs.cwd().deleteTree(test_value_dir) catch {};
    }

    var config = Config.WombatStorageConfig{
        .data_dir = test_dir,
        .value_dir = test_value_dir,
        .mem_table_size = 1024 * 1024, // 1MB
        .num_memtables = 2,
        .max_levels = 3,
        .num_compactors = 1,
        .value_threshold = 512,
        .value_log_file_size = 10 * 1024 * 1024, // 10MB
        .sync_writes = false,
        .block_size = 4096,
        .bloom_false_positive = 0.01,
        .level_size_multiplier = 10,
        .base_table_size = 1024 * 1024, // 1MB
        .num_level_zero_tables = 3,
        .num_level_zero_tables_stall = 6,
        .detect_conflicts = false,
        .verify_checksums = true,
    };

    var adapter = try WombatAdapter.init(allocator, &config);
    defer adapter.deinit();

    // Test basic operations
    try adapter.set("test_key", "test_value");

    const value = try adapter.get("test_key");
    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("test_value", value.?);
    allocator.free(value.?);

    try adapter.delete("test_key");

    const deleted_value = try adapter.get("test_key");
    try std.testing.expect(deleted_value == null);

    // Test health check
    const healthy = try adapter.healthCheck();
    try std.testing.expect(healthy);
}

const std = @import("std");

pub const Config = struct {
    tcp: TcpConfig,
    storage: WombatStorageConfig,
    limits: LimitsConfig,
    auth: AuthConfig,
    cli: CliConfig,

    const TcpConfig = struct {
        host: []const u8,
        port: u16,
        max_connections: u32,
        read_buffer_size: u32,
        write_buffer_size: u32,
    };

    const WombatStorageConfig = struct {
        data_dir: []const u8,
        value_dir: []const u8,
        mem_table_size: usize,
        num_memtables: u32,
        max_levels: u32,
        num_compactors: u32,
        value_threshold: usize,
        value_log_file_size: u64,
        sync_writes: bool,
        block_size: u32,
        bloom_false_positive: f64,
        level_size_multiplier: u32,
        base_table_size: u64,
        num_level_zero_tables: u32,
        num_level_zero_tables_stall: u32,
        detect_conflicts: bool,
        verify_checksums: bool,
    };

    const LimitsConfig = struct {
        max_message_size: u32,
        max_queue_length: u32,
        max_connections: u32,
        max_channels_per_connection: u16,
        heartbeat_interval: u16,
        connection_timeout: u32,
        max_frame_size: u32,
        channel_max: u16,
    };

    const AuthConfig = struct {
        default_user: []const u8,
        default_password: []const u8,
        password_hash_algorithm: PasswordHashAlgorithm,

        const PasswordHashAlgorithm = enum {
            plain,
            sha256,
            argon2,

            pub fn jsonStringify(self: PasswordHashAlgorithm, writer: anytype) !void {
                try writer.write("\"");
                try writer.write(@tagName(self));
                try writer.write("\"");
            }

            pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !PasswordHashAlgorithm {
                const str = try std.json.innerParse([]const u8, allocator, source, options);
                defer allocator.free(str);

                if (std.mem.eql(u8, str, "plain")) return .plain;
                if (std.mem.eql(u8, str, "sha256")) return .sha256;
                if (std.mem.eql(u8, str, "argon2")) return .argon2;

                return error.UnknownField;
            }
        };
    };

    const CliConfig = struct {
        enabled: bool,
        socket_path: []const u8,
        timeout: u32,
        require_auth: bool,
        max_connections: u32,
    };

    pub fn default(allocator: std.mem.Allocator) !Config {
        return Config{
            .tcp = TcpConfig{
                .host = try allocator.dupe(u8, "127.0.0.1"),
                .port = 5672,
                .max_connections = 1000,
                .read_buffer_size = 65536,
                .write_buffer_size = 65536,
            },
            .storage = WombatStorageConfig{
                .data_dir = try allocator.dupe(u8, "./data"),
                .value_dir = try allocator.dupe(u8, "./data/values"),
                .mem_table_size = 64 * 1024 * 1024, // 64MB
                .num_memtables = 3,
                .max_levels = 7,
                .num_compactors = 2,
                .value_threshold = 1024,
                .value_log_file_size = 1024 * 1024 * 1024, // 1GB
                .sync_writes = true,
                .block_size = 4096,
                .bloom_false_positive = 0.01,
                .level_size_multiplier = 10,
                .base_table_size = 2 * 1024 * 1024, // 2MB
                .num_level_zero_tables = 5,
                .num_level_zero_tables_stall = 10,
                .detect_conflicts = false,
                .verify_checksums = true,
            },
            .limits = LimitsConfig{
                .max_message_size = 134217728, // 128MB
                .max_queue_length = 1000000,
                .max_connections = 1000,
                .max_channels_per_connection = 1000,
                .heartbeat_interval = 60,
                .connection_timeout = 600,
                .max_frame_size = 131072, // 128KB
                .channel_max = 1000,
            },
            .auth = AuthConfig{
                .default_user = try allocator.dupe(u8, "guest"),
                .default_password = try allocator.dupe(u8, "guest"),
                .password_hash_algorithm = .argon2,
            },
            .cli = CliConfig{
                .enabled = true,
                .socket_path = try allocator.dupe(u8, "/tmp/yak-cli.sock"),
                .timeout = 30,
                .require_auth = false,
                .max_connections = 10,
            },
        };
    }

    pub fn loadFromFile(allocator: std.mem.Allocator, path: []const u8) !Config {
        const file = std.fs.cwd().openFile(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return error.FileNotFound,
            else => return err,
        };
        defer file.close();

        const contents = try file.readToEndAlloc(allocator, 1024 * 1024); // 1MB max
        defer allocator.free(contents);

        // Parse JSON with environment variable substitution
        const substituted = try substituteEnvVars(allocator, contents);
        defer allocator.free(substituted);

        var parsed = try std.json.parseFromSlice(Config, allocator, substituted, .{
            .allocate = .alloc_always,
        });
        defer parsed.deinit();

        return try cloneConfig(allocator, parsed.value);
    }

    pub fn saveToFile(self: *const Config, allocator: std.mem.Allocator, path: []const u8) !void {
        const json_string = try std.json.stringifyAlloc(allocator, self, .{ .whitespace = .indent_2 });
        defer allocator.free(json_string);

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        try file.writeAll(json_string);
    }

    pub fn deinit(self: *const Config, allocator: std.mem.Allocator) void {
        allocator.free(self.tcp.host);
        allocator.free(self.storage.data_dir);
        allocator.free(self.storage.value_dir);
        allocator.free(self.auth.default_user);
        allocator.free(self.auth.default_password);
        allocator.free(self.cli.socket_path);
    }

    fn cloneConfig(allocator: std.mem.Allocator, config: Config) !Config {
        return Config{
            .tcp = TcpConfig{
                .host = try allocator.dupe(u8, config.tcp.host),
                .port = config.tcp.port,
                .max_connections = config.tcp.max_connections,
                .read_buffer_size = config.tcp.read_buffer_size,
                .write_buffer_size = config.tcp.write_buffer_size,
            },
            .storage = WombatStorageConfig{
                .data_dir = try allocator.dupe(u8, config.storage.data_dir),
                .value_dir = try allocator.dupe(u8, config.storage.value_dir),
                .mem_table_size = config.storage.mem_table_size,
                .num_memtables = config.storage.num_memtables,
                .max_levels = config.storage.max_levels,
                .num_compactors = config.storage.num_compactors,
                .value_threshold = config.storage.value_threshold,
                .value_log_file_size = config.storage.value_log_file_size,
                .sync_writes = config.storage.sync_writes,
                .block_size = config.storage.block_size,
                .bloom_false_positive = config.storage.bloom_false_positive,
                .level_size_multiplier = config.storage.level_size_multiplier,
                .base_table_size = config.storage.base_table_size,
                .num_level_zero_tables = config.storage.num_level_zero_tables,
                .num_level_zero_tables_stall = config.storage.num_level_zero_tables_stall,
                .detect_conflicts = config.storage.detect_conflicts,
                .verify_checksums = config.storage.verify_checksums,
            },
            .limits = config.limits,
            .auth = AuthConfig{
                .default_user = try allocator.dupe(u8, config.auth.default_user),
                .default_password = try allocator.dupe(u8, config.auth.default_password),
                .password_hash_algorithm = config.auth.password_hash_algorithm,
            },
            .cli = CliConfig{
                .enabled = config.cli.enabled,
                .socket_path = try allocator.dupe(u8, config.cli.socket_path),
                .timeout = config.cli.timeout,
                .require_auth = config.cli.require_auth,
                .max_connections = config.cli.max_connections,
            },
        };
    }

    fn substituteEnvVars(allocator: std.mem.Allocator, content: []const u8) ![]u8 {
        var result = std.ArrayList(u8).init(allocator);
        defer result.deinit();

        var i: usize = 0;
        while (i < content.len) {
            if (content[i] == '$' and i + 1 < content.len and content[i + 1] == '{') {
                // Find the closing brace
                const start = i + 2;
                var end = start;
                while (end < content.len and content[end] != '}') {
                    end += 1;
                }

                if (end < content.len) {
                    const var_name = content[start..end];
                    if (std.process.getEnvVarOwned(allocator, var_name)) |env_value| {
                        defer allocator.free(env_value);
                        try result.appendSlice(env_value);
                    } else |_| {
                        // Environment variable not found, keep the original text
                        try result.appendSlice(content[i .. end + 1]);
                    }
                    i = end + 1;
                } else {
                    // No closing brace found, treat as literal
                    try result.append(content[i]);
                    i += 1;
                }
            } else {
                try result.append(content[i]);
                i += 1;
            }
        }

        return result.toOwnedSlice();
    }
};

test "config default creation" {
    const allocator = std.testing.allocator;

    const config = try Config.default(allocator);
    defer config.deinit(allocator);

    try std.testing.expectEqualStrings("127.0.0.1", config.tcp.host);
    try std.testing.expectEqual(@as(u16, 5672), config.tcp.port);
    try std.testing.expectEqual(@as(u32, 1000), config.tcp.max_connections);
}

test "config file save and load" {
    const allocator = std.testing.allocator;

    const config = try Config.default(allocator);
    defer config.deinit(allocator);

    const temp_path = "test_config.json";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    try config.saveToFile(allocator, temp_path);

    const loaded_config = try Config.loadFromFile(allocator, temp_path);
    defer loaded_config.deinit(allocator);

    try std.testing.expectEqualStrings(config.tcp.host, loaded_config.tcp.host);
    try std.testing.expectEqual(config.tcp.port, loaded_config.tcp.port);
}

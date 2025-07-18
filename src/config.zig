const std = @import("std");
const Message = @import("message.zig");
const CompressionType = Message.CompressionType;

pub const Config = struct {
    tcp: TcpConfig,
    ssl: SslConfig,
    storage: WombatStorageConfig,
    limits: LimitsConfig,
    auth: AuthConfig,
    cli: CliConfig,
    metrics: MetricsConfig,
    compression: CompressionConfig,

    const TcpConfig = struct {
        host: []const u8,
        port: u16,
        max_connections: u32,
        read_buffer_size: u32,
        write_buffer_size: u32,
    };

    pub const SslConfig = struct {
        enabled: bool,
        port: u16,
        cert_file: []const u8,
        key_file: []const u8,
        ca_file: ?[]const u8,
        verify_client: bool,
        cipher_suites: []const u8,
        min_protocol_version: TlsVersion,
        max_protocol_version: TlsVersion,

        pub const TlsVersion = enum {
            tls_1_0,
            tls_1_1,
            tls_1_2,
            tls_1_3,

            pub fn jsonStringify(self: TlsVersion, writer: anytype) !void {
                try writer.write("\"");
                try writer.write(@tagName(self));
                try writer.write("\"");
            }

            pub fn jsonParse(allocator: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !TlsVersion {
                const str = try std.json.innerParse([]const u8, allocator, source, options);
                defer allocator.free(str);

                if (std.mem.eql(u8, str, "tls_1_0")) return .tls_1_0;
                if (std.mem.eql(u8, str, "tls_1_1")) return .tls_1_1;
                if (std.mem.eql(u8, str, "tls_1_2")) return .tls_1_2;
                if (std.mem.eql(u8, str, "tls_1_3")) return .tls_1_3;

                return error.UnknownField;
            }
        };
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

    const MetricsConfig = struct {
        enabled: bool,
        host: []const u8,
        port: u16,
        collection_interval_ms: u64,
        prometheus_enabled: bool,
        json_enabled: bool,
        health_check_enabled: bool,
    };

    const CompressionConfig = struct {
        enabled: bool,
        default_type: []const u8, // "gzip", "zlib", "none"
        threshold_bytes: usize,
        level: i32, // Compression level (1-9 for most algorithms)
        auto_compress: bool, // Auto-compress messages over threshold
        whitelist_exchanges: [][]const u8, // Only compress messages from these exchanges
        blacklist_exchanges: [][]const u8, // Never compress messages from these exchanges
        
        pub fn getCompressionType(self: *const CompressionConfig) CompressionType {
            if (!self.enabled) return .none;
            
            if (std.mem.eql(u8, self.default_type, "gzip")) return .gzip;
            if (std.mem.eql(u8, self.default_type, "zlib")) return .zlib;
            return .none;
        }
        
        pub fn shouldCompress(self: *const CompressionConfig, exchange: []const u8, body_size: usize) bool {
            if (!self.enabled or !self.auto_compress or body_size < self.threshold_bytes) {
                return false;
            }
            
            // Check blacklist first
            for (self.blacklist_exchanges) |blacklisted| {
                if (std.mem.eql(u8, exchange, blacklisted)) {
                    return false;
                }
            }
            
            // If whitelist is empty, allow all (except blacklisted)
            if (self.whitelist_exchanges.len == 0) {
                return true;
            }
            
            // Check whitelist
            for (self.whitelist_exchanges) |whitelisted| {
                if (std.mem.eql(u8, exchange, whitelisted)) {
                    return true;
                }
            }
            
            return false;
        }
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
            .ssl = SslConfig{
                .enabled = false,
                .port = 5671,
                .cert_file = try allocator.dupe(u8, "cert.pem"),
                .key_file = try allocator.dupe(u8, "key.pem"),
                .ca_file = null,
                .verify_client = false,
                .cipher_suites = try allocator.dupe(u8, "ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-GCM-SHA256"),
                .min_protocol_version = .tls_1_2,
                .max_protocol_version = .tls_1_3,
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
            .metrics = MetricsConfig{
                .enabled = true,
                .host = try allocator.dupe(u8, "127.0.0.1"),
                .port = 8080,
                .collection_interval_ms = 5000,
                .prometheus_enabled = true,
                .json_enabled = true,
                .health_check_enabled = true,
            },
            .compression = CompressionConfig{
                .enabled = true,
                .default_type = try allocator.dupe(u8, "gzip"),
                .threshold_bytes = 1024, // 1KB
                .level = 6, // Default compression level
                .auto_compress = true,
                .whitelist_exchanges = &[_][]const u8{}, // Empty means allow all
                .blacklist_exchanges = &[_][]const u8{}, // Empty means block none
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
        allocator.free(self.ssl.cert_file);
        allocator.free(self.ssl.key_file);
        if (self.ssl.ca_file) |ca_file| {
            allocator.free(ca_file);
        }
        allocator.free(self.ssl.cipher_suites);
        allocator.free(self.storage.data_dir);
        allocator.free(self.storage.value_dir);
        allocator.free(self.auth.default_user);
        allocator.free(self.auth.default_password);
        allocator.free(self.cli.socket_path);
        allocator.free(self.metrics.host);
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
            .ssl = SslConfig{
                .enabled = config.ssl.enabled,
                .port = config.ssl.port,
                .cert_file = try allocator.dupe(u8, config.ssl.cert_file),
                .key_file = try allocator.dupe(u8, config.ssl.key_file),
                .ca_file = if (config.ssl.ca_file) |ca_file| try allocator.dupe(u8, ca_file) else null,
                .verify_client = config.ssl.verify_client,
                .cipher_suites = try allocator.dupe(u8, config.ssl.cipher_suites),
                .min_protocol_version = config.ssl.min_protocol_version,
                .max_protocol_version = config.ssl.max_protocol_version,
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
            .metrics = MetricsConfig{
                .enabled = config.metrics.enabled,
                .host = try allocator.dupe(u8, config.metrics.host),
                .port = config.metrics.port,
                .collection_interval_ms = config.metrics.collection_interval_ms,
                .prometheus_enabled = config.metrics.prometheus_enabled,
                .json_enabled = config.metrics.json_enabled,
                .health_check_enabled = config.metrics.health_check_enabled,
            },
            .compression = CompressionConfig{
                .enabled = config.compression.enabled,
                .default_type = try allocator.dupe(u8, config.compression.default_type),
                .threshold_bytes = config.compression.threshold_bytes,
                .level = config.compression.level,
                .auto_compress = config.compression.auto_compress,
                .whitelist_exchanges = try cloneStringArray(allocator, config.compression.whitelist_exchanges),
                .blacklist_exchanges = try cloneStringArray(allocator, config.compression.blacklist_exchanges),
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

    fn cloneStringArray(allocator: std.mem.Allocator, strings: [][]const u8) ![][]const u8 {
        var result = try allocator.alloc([]const u8, strings.len);
        for (strings, 0..) |str, i| {
            result[i] = try allocator.dupe(u8, str);
        }
        return result;
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

const std = @import("std");
const wombat = @import("wombat");

const Server = @import("core/server.zig").Server;
const Config = @import("config.zig").Config;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse command line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Load configuration
    const config = try loadConfig(allocator, args);
    defer config.deinit(allocator);

    // Initialize signal handling
    try setupSignalHandlers();

    // Initialize metrics
    initializeMetrics();

    // Create and start server
    var server = try Server.init(allocator, config);
    defer server.deinit();

    std.log.info("Yak AMQP Message Broker starting on {s}:{}", .{ config.tcp.host, config.tcp.port });

    // Start CLI server if enabled
    if (config.cli.enabled) {
        try server.startCliServer();
        std.log.info("CLI server listening on {s}", .{config.cli.socket_path});
    }

    // Start main server
    try server.start();
}

fn loadConfig(allocator: std.mem.Allocator, args: []const []const u8) !Config {
    var config_path: ?[]const u8 = null;

    // Parse command line arguments
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--config") or std.mem.eql(u8, args[i], "-c")) {
            if (i + 1 < args.len) {
                config_path = args[i + 1];
                i += 1;
            } else {
                std.log.err("--config requires a file path", .{});
                std.process.exit(1);
            }
        } else if (std.mem.eql(u8, args[i], "--help") or std.mem.eql(u8, args[i], "-h")) {
            printUsage();
            std.process.exit(0);
        } else if (std.mem.eql(u8, args[i], "--version") or std.mem.eql(u8, args[i], "-v")) {
            std.log.info("Yak AMQP Message Broker v0.1.0", .{});
            std.process.exit(0);
        }
    }

    // Use default config path if not specified
    const final_config_path = config_path orelse "yak.json";

    // Load and parse configuration
    return Config.loadFromFile(allocator, final_config_path) catch |err| switch (err) {
        error.FileNotFound => {
            std.log.info("Config file not found, using defaults: {s}", .{final_config_path});
            return Config.default(allocator);
        },
        else => return err,
    };
}

fn setupSignalHandlers() !void {
    // TODO: Implement proper signal handling for graceful shutdown
    // This is a simplified stub for now
    std.log.info("Signal handlers setup (stub implementation)", .{});
}

var shutdown_requested: bool = false;

fn handleSignal(sig: c_int) callconv(.c) void {
    switch (sig) {
        std.posix.SIG.INT, std.posix.SIG.TERM => {
            std.log.info("Received shutdown signal ({}), initiating graceful shutdown...", .{sig});
            shutdown_requested = true;
        },
        else => {},
    }
}

fn initializeMetrics() void {
    // Initialize metrics collection
    std.log.info("Metrics collection initialized", .{});
}

fn printUsage() void {
    const usage =
        \\Yak - High-Performance AMQP Message Broker
        \\
        \\USAGE:
        \\    yak [OPTIONS]
        \\
        \\OPTIONS:
        \\    -c, --config <FILE>    Configuration file path [default: yak.json]
        \\    -h, --help             Show this help message
        \\    -v, --version          Show version information
        \\
        \\EXAMPLES:
        \\    yak                           # Start with default config
        \\    yak --config /etc/yak.json   # Start with custom config
        \\
    ;
    std.log.info("{s}", .{usage});
}

// Export shutdown flag for other modules
pub fn isShutdownRequested() bool {
    return shutdown_requested;
}

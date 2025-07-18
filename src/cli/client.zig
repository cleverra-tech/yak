const std = @import("std");

const CliClient = struct {
    socket_path: []const u8,
    stream: ?std.net.Stream,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, socket_path: []const u8) CliClient {
        return CliClient{
            .socket_path = socket_path,
            .stream = null,
            .allocator = allocator,
        };
    }

    pub fn connect(self: *CliClient) !void {
        self.stream = try std.net.connectUnixSocket(self.socket_path);

        // Read welcome message
        var buffer: [1024]u8 = undefined;
        const bytes_read = try self.stream.?.read(&buffer);
        const welcome = std.mem.trim(u8, buffer[0..bytes_read], " \t\n\r");
        std.debug.print("{s}\n", .{welcome});
    }

    pub fn disconnect(self: *CliClient) void {
        if (self.stream) |stream| {
            stream.close();
            self.stream = null;
        }
    }

    pub fn sendCommand(self: *CliClient, command: []const u8) ![]u8 {
        const stream = self.stream orelse return error.NotConnected;

        try stream.writeAll(command);
        try stream.writeAll("\n");

        // Read response
        var buffer: [8192]u8 = undefined;
        const bytes_read = try stream.read(&buffer);

        return try self.allocator.dupe(u8, buffer[0..bytes_read]);
    }

    pub fn runInteractive(self: *CliClient) !void {
        // TODO: Implement interactive mode with proper stdin handling
        _ = self;
        std.debug.print("Interactive mode not yet implemented\n", .{});
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var socket_path: []const u8 = "/tmp/yak-cli.sock";
    var interactive = false;
    var command_args = std.ArrayList([]const u8).init(allocator);
    defer command_args.deinit();

    // Parse command line arguments
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--help") or std.mem.eql(u8, args[i], "-h")) {
            printUsage();
            return;
        } else if (std.mem.eql(u8, args[i], "--socket") or std.mem.eql(u8, args[i], "-s")) {
            if (i + 1 < args.len) {
                socket_path = args[i + 1];
                i += 1;
            } else {
                std.debug.print("Error: --socket requires a path\n", .{});
                return;
            }
        } else if (std.mem.eql(u8, args[i], "--interactive") or std.mem.eql(u8, args[i], "-i")) {
            interactive = true;
        } else {
            try command_args.append(args[i]);
        }
    }

    var client = CliClient.init(allocator, socket_path);

    // Connect to CLI server
    client.connect() catch |err| {
        std.debug.print("Failed to connect to Yak CLI server at {s}: {}\n", .{ socket_path, err });
        std.debug.print("Make sure the Yak message broker is running with CLI enabled.\n", .{});
        return;
    };
    defer client.disconnect();

    if (interactive or command_args.items.len == 0) {
        // Interactive mode
        try client.runInteractive();
    } else {
        // Single command mode
        var command_buffer = std.ArrayList(u8).init(allocator);
        defer command_buffer.deinit();

        for (command_args.items, 0..) |arg, idx| {
            if (idx > 0) try command_buffer.append(' ');
            try command_buffer.appendSlice(arg);
        }

        const response = try client.sendCommand(command_buffer.items);
        defer allocator.free(response);

        const clean_response = std.mem.trim(u8, response, " \t\n\r");
        std.debug.print("{s}\n", .{clean_response});
    }
}

fn printUsage() void {
    const usage =
        \\Yak CLI - AMQP Message Broker Administration Tool
        \\
        \\USAGE:
        \\    yak-cli [OPTIONS] [COMMAND] [ARGS...]
        \\
        \\OPTIONS:
        \\    -h, --help              Show this help message
        \\    -s, --socket PATH       Unix socket path [default: /tmp/yak-cli.sock]
        \\    -i, --interactive       Start interactive mode
        \\
        \\COMMANDS:
        \\    help                    Show available commands
        \\    status                  Show broker status
        \\    stats                   Show detailed statistics
        \\    health                  Show health information
        \\
        \\    queue list              List all queues
        \\    queue info <name>       Show queue information
        \\    queue declare <name>    Declare a new queue
        \\    queue delete <name>     Delete a queue
        \\    queue purge <name>      Purge messages from queue
        \\
        \\    exchange list           List all exchanges
        \\    exchange info <name>    Show exchange information
        \\    exchange declare <name> <type>  Declare exchange
        \\    exchange delete <name>  Delete an exchange
        \\
        \\    connection list         List active connections
        \\    connection info <id>    Show connection details
        \\    connection close <id>   Close a connection
        \\
        \\    vhost list              List virtual hosts
        \\    vhost info <name>       Show virtual host details
        \\
        \\EXAMPLES:
        \\    yak-cli status                    # Show broker status
        \\    yak-cli queue list                # List all queues
        \\    yak-cli -i                        # Start interactive mode
        \\    yak-cli -s /custom/path.sock help # Use custom socket path
        \\
    ;
    std.debug.print("{s}", .{usage});
}

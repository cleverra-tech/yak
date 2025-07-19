const std = @import("std");
const Message = @import("../message.zig").Message;
const VirtualHost = @import("../core/vhost.zig").VirtualHost;
const Config = @import("../config.zig").Config;
pub const ClusterConfig = Config.ClusterConfig;

pub const NodeId = u64;
pub const ClusterVersion = u64;

pub const NodeStatus = enum {
    initializing,
    active,
    suspect,
    failed,
    leaving,
};

pub const NodeInfo = struct {
    id: NodeId,
    address: []const u8,
    port: u16,
    status: NodeStatus,
    last_seen: i64,
    version: ClusterVersion,

    pub fn deinit(self: *NodeInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.address);
    }
};

pub const ClusterMessage = struct {
    pub const Type = enum {
        join_request,
        join_response,
        heartbeat,
        node_status_update,
        message_replicate,
        leader_election,
        leader_announce,
    };

    type: Type,
    source_node: NodeId,
    target_node: ?NodeId, // null means broadcast
    sequence: u64,
    timestamp: i64,
    data: []const u8,

    pub fn deinit(self: *ClusterMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};


pub const Cluster = struct {
    allocator: std.mem.Allocator,
    config: ClusterConfig,
    local_node: NodeInfo,
    nodes: std.HashMap(NodeId, *NodeInfo, std.hash_map.AutoContext(NodeId), std.hash_map.default_max_load_percentage),
    leader_node: ?NodeId,
    is_leader: bool,
    sequence_counter: std.atomic.Value(u64),
    server_socket: ?std.net.Server,
    client_connections: std.HashMap(NodeId, std.net.Stream, std.hash_map.AutoContext(NodeId), std.hash_map.default_max_load_percentage),
    running: std.atomic.Value(bool),
    heartbeat_thread: ?std.Thread,
    server_thread: ?std.Thread,
    mutex: std.Thread.Mutex,
    vhost_ref: ?*VirtualHost, // Reference to the main vhost for message handling

    pub fn init(allocator: std.mem.Allocator, config: ClusterConfig) !Cluster {
        if (!config.enabled) {
            return Cluster{
                .allocator = allocator,
                .config = config,
                .local_node = NodeInfo{
                    .id = 0,
                    .address = &[_]u8{},
                    .port = 0,
                    .status = .failed,
                    .last_seen = 0,
                    .version = 0,
                },
                .nodes = std.HashMap(NodeId, *NodeInfo, std.hash_map.AutoContext(NodeId), std.hash_map.default_max_load_percentage).init(allocator),
                .leader_node = null,
                .is_leader = false,
                .sequence_counter = std.atomic.Value(u64).init(0),
                .server_socket = null,
                .client_connections = std.HashMap(NodeId, std.net.Stream, std.hash_map.AutoContext(NodeId), std.hash_map.default_max_load_percentage).init(allocator),
                .running = std.atomic.Value(bool).init(false),
                .heartbeat_thread = null,
                .server_thread = null,
                .mutex = std.Thread.Mutex{},
                .vhost_ref = null,
            };
        }

        const local_node = NodeInfo{
            .id = config.node_id,
            .address = try allocator.dupe(u8, config.bind_address),
            .port = config.bind_port,
            .status = .initializing,
            .last_seen = std.time.timestamp(),
            .version = 1,
        };

        return Cluster{
            .allocator = allocator,
            .config = config,
            .local_node = local_node,
            .nodes = std.HashMap(NodeId, *NodeInfo, std.hash_map.AutoContext(NodeId), std.hash_map.default_max_load_percentage).init(allocator),
            .leader_node = null,
            .is_leader = false,
            .sequence_counter = std.atomic.Value(u64).init(1),
            .server_socket = null,
            .client_connections = std.HashMap(NodeId, std.net.Stream, std.hash_map.AutoContext(NodeId), std.hash_map.default_max_load_percentage).init(allocator),
            .running = std.atomic.Value(bool).init(false),
            .heartbeat_thread = null,
            .server_thread = null,
            .mutex = std.Thread.Mutex{},
            .vhost_ref = null,
        };
    }

    pub fn deinit(self: *Cluster) void {
        if (!self.config.enabled) return;

        self.stop();
        
        // Clean up nodes
        var iterator = self.nodes.valueIterator();
        while (iterator.next()) |node| {
            node.*.deinit(self.allocator);
            self.allocator.destroy(node.*);
        }
        self.nodes.deinit();

        // Clean up connections
        var conn_iterator = self.client_connections.valueIterator();
        while (conn_iterator.next()) |stream| {
            stream.close();
        }
        self.client_connections.deinit();

        self.local_node.deinit(self.allocator);
    }

    pub fn start(self: *Cluster, vhost: *VirtualHost) !void {
        if (!self.config.enabled) return;

        self.vhost_ref = vhost;
        self.running.store(true, .monotonic);

        // Start cluster server
        try self.startServer();

        // Connect to seed nodes
        try self.connectToSeedNodes();

        // Start heartbeat thread
        self.heartbeat_thread = try std.Thread.spawn(.{}, heartbeatWorker, .{self});

        // Update local node status
        self.mutex.lock();
        defer self.mutex.unlock();
        self.local_node.status = .active;

        std.log.info("Cluster node {} started on {}:{}", .{ self.config.node_id, self.config.bind_address, self.config.bind_port });
    }

    pub fn stop(self: *Cluster) void {
        if (!self.config.enabled) return;

        self.running.store(false, .monotonic);

        if (self.heartbeat_thread) |thread| {
            thread.join();
            self.heartbeat_thread = null;
        }

        if (self.server_thread) |thread| {
            thread.join();
            self.server_thread = null;
        }

        if (self.server_socket) |*server| {
            server.deinit();
            self.server_socket = null;
        }

        std.log.info("Cluster node {} stopped", .{self.config.node_id});
    }

    fn startServer(self: *Cluster) !void {
        const address = std.net.Address.parseIp(self.config.bind_address, self.config.bind_port) catch |err| {
            std.log.err("Failed to parse cluster bind address {}:{}: {}", .{ self.config.bind_address, self.config.bind_port, err });
            return err;
        };

        self.server_socket = try address.listen(.{ .reuse_address = true });
        self.server_thread = try std.Thread.spawn(.{}, serverWorker, .{self});
    }

    fn connectToSeedNodes(self: *Cluster) !void {
        for (self.config.seed_nodes) |seed_node| {
            self.connectToNode(seed_node) catch |err| {
                std.log.warn("Failed to connect to seed node {s}: {}", .{ seed_node, err });
                // Continue trying other seed nodes
            };
        }
    }

    fn connectToNode(self: *Cluster, node_address: []const u8) !void {
        // Parse "host:port" format
        const colon_index = std.mem.indexOf(u8, node_address, ":") orelse return error.InvalidAddress;
        const host = node_address[0..colon_index];
        const port_str = node_address[colon_index + 1 ..];
        const port = std.fmt.parseInt(u16, port_str, 10) catch return error.InvalidPort;

        const address = std.net.Address.parseIp(host, port) catch return error.InvalidAddress;
        const stream = std.net.tcpConnectToAddress(address) catch return error.ConnectionFailed;

        // Send join request
        try self.sendJoinRequest(stream);
    }

    fn sendJoinRequest(self: *Cluster, stream: std.net.Stream) !void {
        const join_data = try std.json.stringifyAlloc(self.allocator, .{
            .node_id = self.local_node.id,
            .address = self.local_node.address,
            .port = self.local_node.port,
            .version = self.local_node.version,
        }, .{});
        defer self.allocator.free(join_data);

        const message = ClusterMessage{
            .type = .join_request,
            .source_node = self.local_node.id,
            .target_node = null,
            .sequence = self.sequence_counter.fetchAdd(1, .monotonic),
            .timestamp = std.time.timestamp(),
            .data = join_data,
        };

        try self.sendMessage(stream, message);
    }

    fn sendMessage(self: *Cluster, stream: std.net.Stream, message: ClusterMessage) !void {
        const serialized = try std.json.stringifyAlloc(self.allocator, message, .{});
        defer self.allocator.free(serialized);

        const length = @as(u32, @intCast(serialized.len));
        try stream.writer().writeInt(u32, length, .little);
        try stream.writer().writeAll(serialized);
    }

    pub fn replicateMessage(self: *Cluster, vhost_name: []const u8, queue_name: []const u8, message: *const Message) !void {
        if (!self.config.enabled or self.nodes.count() == 0) return;

        // Create simple JSON manually to avoid HashMap serialization issues
        const replication_data = try std.fmt.allocPrint(self.allocator,
            \\{{"vhost":"{s}","queue":"{s}","message_id":{},"exchange":"{s}","routing_key":"{s}","body":"{s}","persistent":{}}}
        , .{
            vhost_name,
            queue_name,
            message.id,
            message.exchange,
            message.routing_key,
            message.body,
            message.persistent,
        });
        defer self.allocator.free(replication_data);

        const cluster_message = ClusterMessage{
            .type = .message_replicate,
            .source_node = self.local_node.id,
            .target_node = null, // Broadcast to all nodes
            .sequence = self.sequence_counter.fetchAdd(1, .monotonic),
            .timestamp = std.time.timestamp(),
            .data = replication_data,
        };

        // Send to active nodes (limited by replication factor)
        var replicated_count: u8 = 0;
        var node_iterator = self.client_connections.iterator();
        while (node_iterator.next()) |entry| {
            if (replicated_count >= self.config.replication_factor) break;

            self.sendMessage(entry.value_ptr.*, cluster_message) catch |err| {
                std.log.warn("Failed to replicate message to node {}: {}", .{ entry.key_ptr.*, err });
                continue;
            };

            replicated_count += 1;
        }

        if (replicated_count > 0) {
            std.log.debug("Replicated message {} to {} cluster nodes", .{ message.id, replicated_count });
        }
    }

    pub fn getClusterStatus(self: *Cluster) ClusterStatus {
        if (!self.config.enabled) {
            return ClusterStatus{
                .enabled = false,
                .node_count = 0,
                .leader_node = null,
                .is_leader = false,
                .active_connections = 0,
            };
        }

        return ClusterStatus{
            .enabled = true,
            .node_count = @intCast(self.nodes.count() + 1), // +1 for local node
            .leader_node = self.leader_node,
            .is_leader = self.is_leader,
            .active_connections = @intCast(self.client_connections.count()),
        };
    }

    // Worker functions
    fn heartbeatWorker(self: *Cluster) void {
        while (self.running.load(.monotonic)) {
            self.sendHeartbeats() catch |err| {
                std.log.warn("Heartbeat error: {}", .{err});
            };

            std.Thread.sleep(self.config.heartbeat_interval_ms * std.time.ns_per_ms);
        }
    }

    fn serverWorker(self: *Cluster) void {
        while (self.running.load(.monotonic)) {
            if (self.server_socket) |*server| {
                const connection = server.accept() catch |err| {
                    if (self.running.load(.monotonic)) {
                        std.log.warn("Cluster server accept error: {}", .{err});
                    }
                    continue;
                };

                // Handle connection in a separate thread
                const thread = std.Thread.spawn(.{}, handleConnection, .{ self, connection }) catch |err| {
                    std.log.err("Failed to spawn connection handler: {}", .{err});
                    connection.stream.close();
                    continue;
                };
                thread.detach();
            }
        }
    }

    fn handleConnection(self: *Cluster, connection: std.net.Server.Connection) void {
        defer connection.stream.close();

        while (self.running.load(.monotonic)) {
            const message = self.receiveMessage(connection.stream) catch |err| {
                if (self.running.load(.monotonic)) {
                    std.log.debug("Connection receive error: {}", .{err});
                }
                break;
            };
            defer message.deinit(self.allocator);

            self.handleClusterMessage(message, connection.stream) catch |err| {
                std.log.warn("Failed to handle cluster message: {}", .{err});
            };
        }
    }

    fn receiveMessage(self: *Cluster, stream: std.net.Stream) !ClusterMessage {
        const length = try stream.reader().readInt(u32, .little);
        if (length > 1024 * 1024) return error.MessageTooLarge; // 1MB limit

        const data = try self.allocator.alloc(u8, length);
        errdefer self.allocator.free(data);

        try stream.reader().readNoEof(data);

        const parsed = std.json.parseFromSlice(ClusterMessage, self.allocator, data, .{}) catch return error.InvalidMessage;
        defer parsed.deinit();

        var message = parsed.value;
        message.data = try self.allocator.dupe(u8, message.data);

        return message;
    }

    fn handleClusterMessage(self: *Cluster, message: ClusterMessage, response_stream: std.net.Stream) !void {
        switch (message.type) {
            .join_request => try self.handleJoinRequest(message, response_stream),
            .join_response => try self.handleJoinResponse(message),
            .heartbeat => try self.handleHeartbeat(message),
            .message_replicate => try self.handleMessageReplication(message),
            .leader_election => try self.handleLeaderElection(message),
            .leader_announce => try self.handleLeaderAnnouncement(message),
            else => std.log.warn("Unhandled cluster message type: {}", .{message.type}),
        }
    }

    fn handleJoinRequest(self: *Cluster, message: ClusterMessage, response_stream: std.net.Stream) !void {
        std.log.info("Received join request from node {}", .{message.source_node});

        // Add the new node to our cluster
        const node_data = std.json.parseFromSlice(struct {
            node_id: NodeId,
            address: []const u8,
            port: u16,
            version: ClusterVersion,
        }, self.allocator, message.data, .{}) catch return;
        defer node_data.deinit();

        const new_node = try self.allocator.create(NodeInfo);
        new_node.* = NodeInfo{
            .id = node_data.value.node_id,
            .address = try self.allocator.dupe(u8, node_data.value.address),
            .port = node_data.value.port,
            .status = .active,
            .last_seen = std.time.timestamp(),
            .version = node_data.value.version,
        };

        self.mutex.lock();
        defer self.mutex.unlock();
        try self.nodes.put(new_node.id, new_node);

        // Send join response
        const response_data = try std.json.stringifyAlloc(self.allocator, .{
            .node_id = self.local_node.id,
            .accepted = true,
            .leader_node = self.leader_node,
        }, .{});
        defer self.allocator.free(response_data);

        const response = ClusterMessage{
            .type = .join_response,
            .source_node = self.local_node.id,
            .target_node = message.source_node,
            .sequence = self.sequence_counter.fetchAdd(1, .monotonic),
            .timestamp = std.time.timestamp(),
            .data = response_data,
        };

        try self.sendMessage(response_stream, response);
    }

    fn handleJoinResponse(self: *Cluster, message: ClusterMessage) !void {
        _ = self;
        std.log.info("Received join response from node {}", .{message.source_node});
        // Handle successful join
    }

    fn handleHeartbeat(self: *Cluster, message: ClusterMessage) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.nodes.getPtr(message.source_node)) |node| {
            node.*.last_seen = std.time.timestamp();
            if (node.*.status == .suspect) {
                node.*.status = .active;
                std.log.info("Node {} recovered from suspect state", .{message.source_node});
            }
        }
    }

    fn handleMessageReplication(self: *Cluster, message: ClusterMessage) !void {
        if (self.vhost_ref == null) return;

        const repl_data = std.json.parseFromSlice(struct {
            vhost: []const u8,
            queue: []const u8,
            message_id: u64,
            exchange: []const u8,
            routing_key: []const u8,
            body: []const u8,
            persistent: bool,
        }, self.allocator, message.data, .{}) catch return;
        defer repl_data.deinit();

        // Create replicated message
        var replicated_message = Message.init(self.allocator, repl_data.value.message_id, repl_data.value.exchange, repl_data.value.routing_key, repl_data.value.body) catch return;
        replicated_message.persistent = repl_data.value.persistent;

        // Route the replicated message
        self.vhost_ref.?.routeMessage(repl_data.value.exchange, &replicated_message) catch |err| {
            std.log.warn("Failed to route replicated message: {}", .{err});
        };

        std.log.debug("Processed replicated message {} from node {}", .{ repl_data.value.message_id, message.source_node });
    }

    fn handleLeaderElection(self: *Cluster, message: ClusterMessage) !void {
        // Simple leader election: highest node ID wins
        if (message.source_node > self.local_node.id) {
            self.is_leader = false;
            self.leader_node = message.source_node;
        }
    }

    fn handleLeaderAnnouncement(self: *Cluster, message: ClusterMessage) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        
        self.leader_node = message.source_node;
        self.is_leader = (message.source_node == self.local_node.id);
        
        std.log.info("Node {} is now the cluster leader", .{message.source_node});
    }

    fn sendHeartbeats(self: *Cluster) !void {
        const heartbeat_data = try std.json.stringifyAlloc(self.allocator, .{
            .node_id = self.local_node.id,
            .status = self.local_node.status,
            .timestamp = std.time.timestamp(),
        }, .{});
        defer self.allocator.free(heartbeat_data);

        const heartbeat = ClusterMessage{
            .type = .heartbeat,
            .source_node = self.local_node.id,
            .target_node = null,
            .sequence = self.sequence_counter.fetchAdd(1, .monotonic),
            .timestamp = std.time.timestamp(),
            .data = heartbeat_data,
        };

        var conn_iterator = self.client_connections.valueIterator();
        while (conn_iterator.next()) |stream| {
            self.sendMessage(stream.*, heartbeat) catch |err| {
                std.log.debug("Failed to send heartbeat: {}", .{err});
            };
        }
    }
};

pub const ClusterStatus = struct {
    enabled: bool,
    node_count: u32,
    leader_node: ?NodeId,
    is_leader: bool,
    active_connections: u32,
};
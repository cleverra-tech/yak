# Yak AMQP Message Broker

A high-performance AMQP 0-9-1 message broker implemented in Zig, featuring the Wombat storage engine for persistence.

## Features

- **Complete AMQP 0-9-1 Protocol**: Full support for exchanges, queues, bindings, and routing
- **High-Performance Storage**: Integrated with Wombat LSM-tree storage engine
- **Virtual Hosts**: Multi-tenant support with isolated messaging environments  
- **Authentication System**: Multiple password hash algorithms (plain, SHA-256, Argon2)
- **CLI Management**: Unix socket-based administration interface
- **Flexible Configuration**: JSON config with environment variable substitution
- **Modern Zig**: Built with Zig's package manager for clean dependencies

## Quick Start

### Prerequisites

- Zig 0.15.0-dev.1034+bd97b6618 or later
- Git

### Building

```bash
git clone https://github.com/cleverra-tech/yak.git
cd yak
zig build
```

### Running

```bash
# Start the broker with default config
./zig-out/bin/yak

# Start with custom config
./zig-out/bin/yak --config /path/to/config.json

# Use CLI management tool
./zig-out/bin/yak-cli status
./zig-out/bin/yak-cli queue list
```

## Configuration

Create a `yak.json` configuration file:

```json
{
  "tcp": {
    "host": "127.0.0.1",
    "port": 5672,
    "max_connections": 1000
  },
  "storage": {
    "data_dir": "./data",
    "mem_table_size": 67108864,
    "sync_writes": true
  },
  "auth": {
    "default_user": "guest",
    "default_password": "guest",
    "password_hash_algorithm": "argon2"
  },
  "cli": {
    "enabled": true,
    "socket_path": "/tmp/yak-cli.sock"
  }
}
```

Environment variables can be used with `${VAR_NAME}` syntax.

## Architecture

### Core Components

- **Virtual Hosts**: Namespace isolation for exchanges and queues
- **Exchanges**: Message routing (direct, fanout, topic, headers)
- **Queues**: Message storage with consumer management
- **Bindings**: Routing rules between exchanges and queues
- **Storage**: Wombat LSM-tree engine for persistence

### CLI Management

```bash
# Queue operations
yak-cli queue declare my-queue
yak-cli queue info my-queue
yak-cli queue purge my-queue

# Exchange operations  
yak-cli exchange declare my-exchange direct
yak-cli exchange list

# System monitoring
yak-cli stats
yak-cli health
```

## Development

### Testing

```bash
# Run unit tests
zig build test

# Run integration tests
zig build integration

# Run all tests
zig build test-all
```

### Benchmarks

```bash
zig build benchmark
```

### Code Quality

The project uses pre-commit hooks for:
- Code formatting (`zig fmt`)
- Build validation
- Test execution
- Conventional commit messages

## Protocol Support

Implements AMQP 0-9-1 specification including:

- Connection management and authentication
- Channel multiplexing
- Exchange declaration and deletion
- Queue declaration, binding, and management
- Message publishing and consumption
- Acknowledgments and rejections
- Quality of Service (QoS) controls

## Storage Engine

Yak integrates with the Wombat storage engine, providing:

- LSM-tree based persistence
- High write throughput
- Efficient range queries
- Configurable compaction strategies
- Crash recovery

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes following the code style
4. Ensure all tests pass
5. Submit a pull request

Commit messages must follow conventional commit format:
- `feat: add new feature`
- `fix: resolve bug`
- `docs: update documentation`

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Performance

Yak is designed for high throughput messaging workloads:

- Concurrent connection handling
- Efficient message routing algorithms
- Memory-mapped storage with Wombat
- Lock-free data structures where possible

## Roadmap

See the [Issues](https://github.com/cleverra-tech/yak/issues) for planned features and improvements.

## Support

- File issues on GitHub for bugs and feature requests
- Check the CLI help: `yak-cli help`
- Review configuration options: `yak --help`
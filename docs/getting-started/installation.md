# Installation

This guide covers the various ways to install Housekeeper on your system.

## Prerequisites

- **ClickHouse**: Version 20.8 or later
- **Go**: Version 1.21 or later (if building from source)
- **Docker**: For container usage and migration testing (optional)

## Installation Methods

### Go Install (Recommended)

The easiest way to install Housekeeper is using Go's built-in package manager:

```bash
go install github.com/pseudomuto/housekeeper@latest
```

This will install the latest version of Housekeeper to your `$GOPATH/bin` directory.

!!! tip "Add to PATH"
    Make sure `$GOPATH/bin` is in your system's PATH to use `housekeeper` from anywhere.

### Download Pre-built Binaries

Download the latest release from GitHub:

1. Visit the [releases page](https://github.com/pseudomuto/housekeeper/releases)
2. Download the appropriate binary for your platform
3. Extract and place the binary in your PATH

#### Linux/macOS
```bash
# Download and install (replace VERSION with actual version)
curl -L https://github.com/pseudomuto/housekeeper/releases/download/vVERSION/housekeeper_linux_amd64.tar.gz | tar xz
sudo mv housekeeper /usr/local/bin/
```

### Container Usage

Housekeeper is available as a container image for use in CI/CD pipelines or containerized environments:

```bash
# Pull the latest image
docker pull ghcr.io/pseudomuto/housekeeper:latest

# Run with help
docker run --rm ghcr.io/pseudomuto/housekeeper:latest --help

# Run diff command with mounted project directory
docker run --rm \
  -v $(pwd):/project \
  ghcr.io/pseudomuto/housekeeper:latest \
  diff --dir /project

# Use specific version
docker run --rm ghcr.io/pseudomuto/housekeeper:v1.0.0 --version
```

### Build from Source

For the latest development features or to contribute to the project:

```bash
# Clone the repository
git clone https://github.com/pseudomuto/housekeeper.git
cd housekeeper

# Build using the task runner
go install github.com/go-task/task/v3/cmd/task@latest
task build

# Or build manually
go build -o housekeeper cmd/housekeeper/main.go
```

## Verify Installation

After installation, verify that Housekeeper is working correctly:

```bash
# Check version
housekeeper --version

# View help
housekeeper --help

# Test with a simple command
housekeeper init --help
```

You should see output similar to:
```
Housekeeper v1.0.0
A modern ClickHouse schema management tool
```

## Next Steps

Once Housekeeper is installed, you're ready to:

- [Get started with your first project](quick-start.md)
- [Set up a new project structure](project-setup.md)
- [Learn about schema management](../user-guide/schema-management.md)

## Troubleshooting

### Command Not Found

If you get a "command not found" error:

1. **Go Install**: Ensure `$GOPATH/bin` is in your PATH
2. **Binary Install**: Verify the binary is executable and in your PATH
3. **Container**: Use the full Docker command instead of the binary

### Permission Denied

If you get permission errors:

```bash
# Make the binary executable
chmod +x housekeeper

# Or install to a directory you own
cp housekeeper ~/.local/bin/
```

### Docker Issues

If Docker commands fail:

1. Ensure Docker is running: `docker ps`
2. Check image availability: `docker images ghcr.io/pseudomuto/housekeeper`
3. Pull explicitly: `docker pull ghcr.io/pseudomuto/housekeeper:latest`

For more troubleshooting help, see the [Troubleshooting Guide](../advanced/troubleshooting.md) or [open an issue](https://github.com/pseudomuto/housekeeper/issues).
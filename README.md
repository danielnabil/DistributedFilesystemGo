# Distributed File System in Go

A high-performance distributed file system implementation in Go, designed for reliable and fast file uploads and downloads across multiple storage nodes.

## Features

- **Chunk-Based File Transfers**: Files are split into chunks for parallel processing, improving transfer speeds significantly
- **Node Replication**: Each file chunk is automatically replicated across multiple storage nodes for fault tolerance
- **Concurrent Operations**: Leverages Go's goroutines and channels for efficient parallel chunk processing
- **Master-Tracker Architecture**: Centralized coordination for file metadata and node management
- **Data Keeper Nodes**: Distributed storage nodes that handle actual file chunk storage and retrieval
- **Fault Tolerance**: Automatic failover when nodes become unavailable, with backup data serving

## Architecture

```
                              ┌───────────────────────────────────────┐
                              │            MASTER TRACKER             │
                              │  ┌─────────────────────────────────┐  │
                              │  │  • File Metadata Management     │  │
                              │  │  • Chunk Location Tracking      │  │
                              │  │  • Node Health Monitoring       │  │
                              │  │  • Replication Coordination     │  │
                              │  └─────────────────────────────────┘  │
                              └───────────────┬───────────────────────┘
                                              │
                         ┌────────────────────┼────────────────────┐
                         │                    │                    │
                         ▼                    ▼                    ▼
              ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
              │  DATA KEEPER 1   │ │  DATA KEEPER 2   │ │  DATA KEEPER 3   │
              │  ┌────────────┐  │ │  ┌────────────┐  │ │  ┌────────────┐  │
              │  │  Chunk A   │  │ │  │  Chunk B   │  │ │  │  Chunk C   │  │
              │  │  Chunk D   │  │ │  │  Chunk A'  │  │ │  │  Chunk B'  │  │
              │  │  Chunk C'  │  │ │  │  Chunk D'  │  │ │  │  Chunk A'' │  │
              │  └────────────┘  │ │  └────────────┘  │ │  └────────────┘  │
              └────────┬─────────┘ └────────┬─────────┘ └────────┬─────────┘
                       │                    │                    │
                       └────────────────────┼────────────────────┘
                                            │
                              ◄─── Replication Sync ───►
                                            │
                              ┌─────────────┴─────────────┐
                              │          CLIENT           │
                              │  ┌─────────────────────┐  │
                              │  │ Upload: Split file  │  │
                              │  │ into chunks & send  │  │
                              │  │ in parallel         │  │
                              │  ├─────────────────────┤  │
                              │  │ Download: Fetch     │  │
                              │  │ chunks & reassemble │  │
                              │  └─────────────────────┘  │
                              └───────────────────────────┘

Legend:
  Chunk X   = Primary copy
  Chunk X'  = First replica
  Chunk X'' = Second replica
```

### Components

- **Client**: Handles file upload/download requests, splits files into chunks, and reassembles downloaded chunks
- **Master Tracker**: Manages file metadata, tracks chunk locations across data keepers, and coordinates replication
- **Data Keepers**: Storage nodes that store file chunks and handle replication to peer nodes

## Getting Started

### Prerequisites

- Go 1.19 or higher

### Installation

```bash
git clone https://github.com/danielnabil/DistributedFilesystemGo.git
cd DistributedFilesystemGo
go mod download
```

### Running the System

1. **Start the Master Tracker**:
```bash
cd master_tracker
go run main.go
```

2. **Start Data Keeper Nodes** (in separate terminals):
```bash
cd data_keeper
go run main.go -port 8001
go run main.go -port 8002
go run main.go -port 8003
```

3. **Run the Client**:
```bash
cd client
go run main.go
```

## Usage

### Upload a File

```go
// Upload a file to the distributed file system
client.Upload("path/to/local/file.txt")
```

### Download a File

```go
// Download a file from the distributed file system
client.Download("remote_filename.txt", "path/to/save/file.txt")
```

## How It Works

### Upload Process

1. Client splits the file into fixed-size chunks
2. Client requests upload allocation from Master Tracker
3. Master Tracker assigns Data Keepers for each chunk
4. Client uploads chunks in parallel to assigned Data Keepers
5. Data Keepers replicate chunks to backup nodes
6. Master Tracker updates metadata upon successful upload

### Download Process

1. Client requests file metadata from Master Tracker
2. Master Tracker returns chunk locations across Data Keepers
3. Client downloads chunks in parallel from available nodes
4. Client reassembles chunks into the original file
5. If a node is unavailable, client fetches from backup nodes

## Project Structure

```
DistributedFilesystemGo/
├── client/           # Client implementation for file operations
├── data_keeper/      # Data Keeper node implementation
├── master_tracker/   # Master Tracker coordination service
├── dfs/              # Core distributed file system logic
├── go.mod            # Go module definition
└── go.sum            # Go module checksums
```

## Technical Highlights

- **Concurrency**: Uses Go's concurrency primitives (goroutines, channels, WaitGroups) for parallel operations
- **Network Communication**: TCP-based communication between components
- **Chunk Management**: Configurable chunk sizes for optimizing transfer performance
- **Metadata Tracking**: Efficient tracking of file chunks and their locations across the cluster

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is open source and available under the [MIT License](LICENSE).

## Author

**Daniel Atallah**
- GitHub: [@danielnabil](https://github.com/danielnabil)
- LinkedIn: [daniel-atallah01](https://linkedin.com/in/daniel-atallah01)


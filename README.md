# TritonTube

Technologies:
- Go (Golang)
- gRPC
- Protocol Buffers
- SQLite

TritonTube is a distributed video storage system that horizontally scales video content across multiple storage servers using consistent hashing. Video files are stored on seperate storage nodes, and the system supports dynamic addition/removal of nodes with automatic file migration. All communication is implemented using **gRPC** with **protobuf**. Metadata is stored with SQLite.

Features:
- Designed and implemented gRPC APIs for file storage and cluster administration
- Built a consistent hashing based storage layer for dynamic video distribution
- Supports automatic file migration
- Videos are parsed with ffmpeg and MPEG-DASH
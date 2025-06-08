package main

import (
	"flag"
	"fmt"
	"net"
	"tritontube/internal/proto"
	"tritontube/internal/storage"

	"google.golang.org/grpc"
)

func main() {
	host := flag.String("host", "localhost", "Host address for the server")
	port := flag.Int("port", 8090, "Port number for the server")
	flag.Parse()

	// Validate arguments
	if *port <= 0 {
		panic("Error: Port number must be positive")
	}

	if flag.NArg() < 1 {
		fmt.Println("Usage: storage [OPTIONS] <baseDir>")
		fmt.Println("Error: Base directory argument is required")
		return
	}
	baseDir := flag.Arg(0)

	fmt.Println("Starting storage server...")
	fmt.Printf("Host: %s\n", *host)
	fmt.Printf("Port: %d\n", *port)
	fmt.Printf("Base Directory: %s\n", baseDir)

	address := fmt.Sprintf("%s:%d", *host, *port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println(err)
		return
	}
	grpcServer := grpc.NewServer()
	proto.RegisterStorageServiceServer(grpcServer, &storage.Server{
		BaseDirectory: baseDir,
	})
	if err := grpcServer.Serve(lis); err != nil {
		fmt.Println(err)
	}
}

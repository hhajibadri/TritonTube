// Lab 8: Implement a network video content service (client using consistent hashing)

package web

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net"
	"sort"
	"strings"
	"sync"
	"tritontube/internal/proto"

	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func hashStringToUint64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

// NetworkVideoContentService implements VideoContentService using a network of nodes.
type NetworkVideoContentService struct {
	hashRing []uint64
	hashMap  map[uint64]*Node
	mu       sync.RWMutex
	proto.UnimplementedVideoContentAdminServiceServer
}

type Node struct {
	address string
	client  proto.StorageServiceClient
}

// Uncomment the following line to ensure NetworkVideoContentService implements VideoContentService
var _ VideoContentService = (*NetworkVideoContentService)(nil)

func (n *NetworkVideoContentService) AddNode(ctx context.Context, req *proto.AddNodeRequest) (*proto.AddNodeResponse, error) {
	addr := req.NodeAddress
	hash := hashStringToUint64(addr)
	n.mu.Lock()
	defer n.mu.Unlock()
	idx := sort.Search(len(n.hashRing), func(i int) bool {
		return n.hashRing[i] >= hash
	})
	if idx < len(n.hashRing) && n.hashRing[idx] == hash {
		return &proto.AddNodeResponse{MigratedFileCount: 0}, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := proto.NewStorageServiceClient(conn)
	newNode := &Node{
		address: addr,
		client:  c,
	}
	n.hashRing = append(n.hashRing, hash)
	n.hashMap[hash] = newNode
	slices.Sort(n.hashRing)

	movedFiles := 0

	for _, h := range n.hashRing {
		if h == hash {
			continue
		}
		node := n.hashMap[h]
		resp, err := node.client.ListFiles(context.Background(), &proto.Empty{})
		if err != nil {
			return nil, err
		}
		for _, filename := range resp.Filenames {
			successor := n.FindSuccessor(filename)
			if successor == nil || successor.address != addr {
				continue
			}
			paths := strings.Split(filename, "/")
			videoId := paths[0]
			file_chunk := paths[1]
			resp, err := node.client.ReadFile(context.Background(), &proto.ReadFileRequest{VideoId: videoId, Filename: file_chunk})
			if err != nil {
				return nil, err
			}
			_, err = successor.client.WriteFile(context.Background(), &proto.WriteFileRequest{VideoId: videoId, Filename: file_chunk, Data: resp.Data})
			if err != nil {
				return nil, err
			}
			_, err = node.client.DeleteFile(context.Background(), &proto.DeleteFileRequest{VideoId: videoId, Filename: file_chunk})
			if err != nil {
				return nil, err
			}
			movedFiles += 1
		}
	}
	return &proto.AddNodeResponse{MigratedFileCount: int32(movedFiles)}, nil
}

func (n *NetworkVideoContentService) RemoveNode(ctx context.Context, req *proto.RemoveNodeRequest) (*proto.RemoveNodeResponse, error) {
	n.mu.Lock()
	addr := req.NodeAddress
	hash := hashStringToUint64(addr)
	defer n.mu.Unlock()
	idx := sort.Search(len(n.hashRing), func(i int) bool {
		return n.hashRing[i] >= hash
	})
	if idx == len(n.hashRing) {
		return &proto.RemoveNodeResponse{MigratedFileCount: 0}, nil
	}
	nodeToDelete := n.hashMap[hash]
	n.hashRing = slices.Delete(n.hashRing, idx, idx+1)
	delete(n.hashMap, hash)

	movedFiles := 0

	resp, err := nodeToDelete.client.ListFiles(context.Background(), &proto.Empty{})
	if err != nil {
		return nil, err
	}

	for _, filename := range resp.Filenames {
		paths := strings.Split(filename, "/")
		videoId := paths[0]
		file_chunk := paths[1]
		successor := n.FindSuccessor(filename)
		if successor == nil {
			continue
		}
		resp, err := nodeToDelete.client.ReadFile(context.Background(), &proto.ReadFileRequest{VideoId: videoId, Filename: file_chunk})
		if err != nil {
			return nil, err
		}
		_, err = successor.client.WriteFile(context.Background(), &proto.WriteFileRequest{VideoId: videoId, Filename: file_chunk, Data: resp.Data})
		if err != nil {
			return nil, err
		}
		_, err = nodeToDelete.client.DeleteFile(context.Background(), &proto.DeleteFileRequest{VideoId: videoId, Filename: file_chunk})
		if err != nil {
			return nil, err
		}
		movedFiles += 1
	}
	return &proto.RemoveNodeResponse{MigratedFileCount: int32(movedFiles)}, nil
}

func (n *NetworkVideoContentService) ListNodes(ctx context.Context, req *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	nodesAddresses := make([]string, len(n.hashRing))
	for idx, hashedNode := range n.hashRing {
		nodesAddresses[idx] = n.hashMap[hashedNode].address
	}
	return &proto.ListNodesResponse{Nodes: nodesAddresses}, nil
}

func NewNetworkVideoContentService(adminAddr string, addresses []string) (*NetworkVideoContentService, error) {
	n := &NetworkVideoContentService{
		hashRing: make([]uint64, 0),
		hashMap:  make(map[uint64]*Node),
	}

	l, err := net.Listen("tcp", adminAddr)
	if err != nil {
		return nil, err
	}
	grpcServer := grpc.NewServer()
	proto.RegisterVideoContentAdminServiceServer(grpcServer, n)

	go grpcServer.Serve(l)

	for _, addr := range addresses {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		node := &Node{address: addr, client: proto.NewStorageServiceClient(conn)}
		hash := hashStringToUint64(addr)
		n.hashRing = append(n.hashRing, hash)
		n.hashMap[hash] = node
	}
	slices.Sort(n.hashRing)
	return n, nil
}

func (n *NetworkVideoContentService) FindSuccessor(key string) *Node {
	hash := hashStringToUint64(key)
	if len(n.hashRing) == 0 {
		return nil
	}
	idx := sort.Search(len(n.hashRing), func(i int) bool {
		return n.hashRing[i] >= hash
	})
	if idx == len(n.hashRing) {
		idx = 0
	}
	return n.hashMap[n.hashRing[idx]]
}

func (n *NetworkVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	key := videoId + "/" + filename
	node := n.FindSuccessor(key)
	if node == nil {
		return nil, errors.New("couldn't find node")
	}
	req := &proto.ReadFileRequest{
		VideoId:  videoId,
		Filename: filename,
	}
	resp, err := node.client.ReadFile(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (n *NetworkVideoContentService) Write(videoId string, filename string, data []byte) error {
	key := videoId + "/" + filename
	node := n.FindSuccessor(key)
	if node == nil {
		return errors.New("couldn't find node")
	}
	req := &proto.WriteFileRequest{
		VideoId:  videoId,
		Filename: filename,
		Data:     data,
	}
	_, err := node.client.WriteFile(context.Background(), req)
	return err
}

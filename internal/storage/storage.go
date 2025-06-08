// Lab 8: Implement a network video content service (server)

package storage

import (
	"context"
	"os"
	"path/filepath"
	"tritontube/internal/proto"
)

// Implement a network video content service (server)
type Server struct {
	proto.UnimplementedStorageServiceServer
	BaseDirectory string
}

func (s *Server) ReadFile(ctx context.Context, req *proto.ReadFileRequest) (*proto.ReadFileResponse, error) {
	path := filepath.Join(s.BaseDirectory, req.GetVideoId(), req.GetFilename())
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return &proto.ReadFileResponse{Data: data}, nil
}

func (s *Server) WriteFile(ctx context.Context, req *proto.WriteFileRequest) (*proto.Empty, error) {
	dir := filepath.Join(s.BaseDirectory, req.GetVideoId())
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, req.Filename)
	if err := os.WriteFile(path, req.Data, 0777); err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}

func (s *Server) DeleteFile(ctx context.Context, req *proto.DeleteFileRequest) (*proto.Empty, error) {
	path := filepath.Join(s.BaseDirectory, req.GetVideoId(), req.GetFilename())
	if err := os.Remove(path); err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}

func (s *Server) ListFiles(ctx context.Context, req *proto.Empty) (*proto.ListFilesResponse, error) {
	dirs, err := os.ReadDir(s.BaseDirectory)
	filenames := make([]string, 0)
	if err != nil {
		return nil, err
	}
	for _, dir := range dirs {
		if dir.IsDir() {
			subDir := filepath.Join(s.BaseDirectory, dir.Name())
			files, err := os.ReadDir(subDir)
			if err != nil {
				return nil, err
			}
			for _, f := range files {
				if !f.IsDir() {
					filenames = append(filenames, dir.Name()+"/"+f.Name())
				}
			}
		}
	}
	return &proto.ListFilesResponse{Filenames: filenames}, nil
}

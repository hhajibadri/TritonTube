// Lab 7: Implement a local filesystem video content service

package web

import (
	"log"
	"os"
	"path/filepath"
)

// FSVideoContentService implements VideoContentService using the local filesystem.
type FSVideoContentService struct {
	storageDirectory string
}

// Uncomment the following line to ensure FSVideoContentService implements VideoContentService
var _ VideoContentService = (*FSVideoContentService)(nil)

func (fs *FSVideoContentService) Initialize(dir string) error {
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		log.Printf("FS Initialize: %v\n", err)
		return err
	}
	fs.storageDirectory = dir
	return nil
}

func (fs *FSVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(fs.storageDirectory, videoId, filename))
	if err != nil {
		log.Printf("FS Read: %v\n", err)
		return nil, err
	}
	return data, nil
}

func (fs *FSVideoContentService) Write(videoId string, filename string, data []byte) error {
	dir := filepath.Join(fs.storageDirectory, videoId)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		log.Printf("FS Write: %v\n", err)
		return err
	}
	err = os.WriteFile(filepath.Join(dir, filename), data, 0777)
	if err != nil {
		log.Printf("FS Write: %v\n", err)
		return err
	}
	return nil
}

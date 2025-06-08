// Lab 7: Implement a web server

package web

import (
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// 256 MBs
const maxSize int64 = 1 << 28

type VideoMetaDataParsed struct {
	Id         string
	EscapedId  string
	UploadTime string
}

type server struct {
	Addr string
	Port int

	metadataService VideoMetadataService
	contentService  VideoContentService

	mux *http.ServeMux
}

func NewServer(
	metadataService VideoMetadataService,
	contentService VideoContentService,
) *server {
	return &server{
		metadataService: metadataService,
		contentService:  contentService,
	}
}

func (s *server) Start(lis net.Listener) error {
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/upload", s.handleUpload)
	s.mux.HandleFunc("/videos/", s.handleVideo)
	s.mux.HandleFunc("/content/", s.handleVideoContent)
	s.mux.HandleFunc("/", s.handleIndex)
	return http.Serve(lis, s.mux)
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "not GET request", http.StatusMethodNotAllowed)
		return
	}
	tmpl, err := template.New("index").Parse(indexHTML)
	if err != nil {
		http.Error(w, "failed to parse template", http.StatusInternalServerError)
		return
	}
	metadatas, err := s.metadataService.List()
	if err != nil {
		http.Error(w, "failed to retrieve metadatas", http.StatusInternalServerError)
		return
	}
	renderedMetadatas := make([]VideoMetaDataParsed, len(metadatas))
	for i, meta := range metadatas {
		renderedMetadatas[i] = VideoMetaDataParsed{
			Id:         meta.Id,
			EscapedId:  url.PathEscape(meta.Id),
			UploadTime: meta.UploadedAt.Format(layout),
		}
	}

	err = tmpl.Execute(w, renderedMetadatas)
	if err != nil {
		log.Println("failed to exectute template")
	}
}

func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "not POST request", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseMultipartForm(maxSize); err != nil {
		http.Error(w, "failed to parse request", http.StatusBadRequest)
		return
	}
	file, head, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "failed to get file", http.StatusBadRequest)
		return
	}
	defer file.Close()
	if filepath.Ext(head.Filename) != ".mp4" {
		http.Error(w, "incorrect file type", http.StatusBadRequest)
		return
	}
	videoId := strings.TrimSuffix(head.Filename, ".mp4")
	if len(videoId) == 0 {
		http.Error(w, "filename of length 0 not allowed", http.StatusBadRequest)
		return
	}
	// Better to check duplicates with read since we could potentially
	// insert an entry with create but the mp4 file doesn't convert
	_, err = s.metadataService.Read(videoId)
	if err == nil {
		http.Error(w, "video already exists with name", http.StatusConflict)
		return
	}
	tempDir, err := os.MkdirTemp("", "tmp-*")
	if err != nil {
		http.Error(w, "failed to create directory", http.StatusConflict)
		return
	}
	defer os.RemoveAll(tempDir)
	videoPath := filepath.Join(tempDir, head.Filename)
	copy, err := os.Create(videoPath)
	if err != nil {
		http.Error(w, "failed to create file", http.StatusInternalServerError)
		return
	}
	defer copy.Close()
	_, err = io.Copy(copy, file)
	if err != nil {
		http.Error(w, "failed to copy file", http.StatusInternalServerError)
		return
	}

	manifestPath := filepath.Join(tempDir, "manifest.mpd")

	cmd := exec.Command(
		"ffmpeg",
		"-i", videoPath, // input file
		"-c:v", "libx264", // video codec
		"-c:a", "aac", // audio codec
		"-bf", "1", // max 1 b-frame
		"-keyint_min", "120", // minimum keyframe interval
		"-g", "120", // keyframe every 120 frames
		"-sc_threshold", "0", // scene change threshold
		"-b:v", "3000k", // video bitrate
		"-b:a", "128k", // audio bitrate
		"-f", "dash", // dash format
		"-use_timeline", "1", // use timeline
		"-use_template", "1", // use template
		"-init_seg_name", "init-$RepresentationID$.m4s", // init segment naming
		"-media_seg_name", "chunk-$RepresentationID$-$Number%05d$.m4s", // media segment naming
		"-seg_duration", "4", // segment duration in seconds
		manifestPath) // output file

	err = cmd.Run()
	if err != nil {
		http.Error(w, "failed to convert .mp4", http.StatusInternalServerError)
		return
	}
	// insert only after mp4 file is created
	err = s.metadataService.Create(videoId, time.Now())
	if err != nil {
		http.Error(w, "failed to insert video id & time", http.StatusConflict)
		return
	}

	files, err := os.ReadDir(tempDir)
	if err != nil {
		http.Error(w, "failed to get files", http.StatusInternalServerError)
		return
	}

	for _, f := range files {
		if f.Name() == head.Filename {
			continue
		}
		data, err := os.ReadFile(filepath.Join(tempDir, f.Name()))
		if err != nil {
			http.Error(w, "failed to iterate through files", http.StatusInternalServerError)
			return
		}
		err = s.contentService.Write(videoId, f.Name(), data)
		if err != nil {
			http.Error(w, "failed to copy over files", http.StatusInternalServerError)
			return
		}

	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *server) handleVideo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "not GET request", http.StatusMethodNotAllowed)
		return
	}
	videoId := r.URL.Path[len("/videos/"):]
	log.Println("Video ID:", videoId)
	tmpl, err := template.New("video").Parse(videoHTML)
	if err != nil {
		log.Println("Failed to parse html")
		return
	}
	metadata, err := s.metadataService.Read(videoId)
	if err != nil {
		log.Println("Failed to get metadata or video does not exist")
		http.Error(w, "video does not exist", http.StatusNotFound)
		return
	}
	_ = tmpl.Execute(w, metadata)
}

func (s *server) handleVideoContent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "not GET request", http.StatusBadRequest)
		return
	}
	// parse /content/<videoId>/<filename>
	videoId := r.URL.Path[len("/content/"):]
	parts := strings.Split(videoId, "/")
	if len(parts) != 2 {
		http.Error(w, "Invalid content path", http.StatusBadRequest)
		return
	}
	videoId = parts[0]
	filename := parts[1]
	log.Println("Video ID:", videoId, "Filename:", filename)
	file, err := s.contentService.Read(videoId, filename)
	if err != nil {
		http.Error(w, "failed to get files", http.StatusInternalServerError)
		return
	}
	if filename == "manifest.mpd" {
		w.Header().Add("Content-Type", "application/dash+xml")
		w.Header().Add("Content-Length", strconv.Itoa(len(file)))
	} else {
		w.Header().Add("Content-Type", "video/m4s")
		w.Header().Add("Content-Length", strconv.Itoa(len(file)))
	}
	w.Write(file)
}

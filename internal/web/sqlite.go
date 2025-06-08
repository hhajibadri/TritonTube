// Lab 7: Implement a SQLite video metadata service

package web

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const layout string = "2006-01-02 15:04:05"

type SQLiteVideoMetadataService struct {
	db *sql.DB
}

// Uncomment the following line to ensure SQLiteVideoMetadataService implements VideoMetadataService
var _ VideoMetadataService = (*SQLiteVideoMetadataService)(nil)

func (s *SQLiteVideoMetadataService) Initialize(database string) error {
	const createTable string = `
CREATE TABLE IF NOT EXISTS videos (
  id TEXT PRIMARY KEY,
  time TEXT NOT NULL
);`
	var err error
	if s.db, err = sql.Open("sqlite3", database); err != nil {
		log.Printf("SQL Open: %v\n", err)
		return err
	}
	if _, err = s.db.Exec(createTable); err != nil {
		log.Printf("SQL Exec: %v\n", err)
		return err
	}
	log.Println("Table Created/Opened")
	return nil
}

func (s *SQLiteVideoMetadataService) Close() error {
	return s.db.Close()
}

func (s *SQLiteVideoMetadataService) Read(id string) (*VideoMetadata, error) {
	row := s.db.QueryRow("SELECT id, time FROM videos WHERE id = ?", id)
	var metadata VideoMetadata
	var uploadedTime string
	var err error
	if err = row.Scan(&metadata.Id, &uploadedTime); err != nil {
		log.Printf("SQL Scan -- %v\n", err)
		return nil, err
	}
	if metadata.UploadedAt, err = time.Parse(layout, uploadedTime); err != nil {
		log.Printf("Time Parse -- %v\n", err)
		return nil, err
	}
	return &metadata, nil
}

func (s *SQLiteVideoMetadataService) List() ([]VideoMetadata, error) {
	var metadatas []VideoMetadata
	rows, err := s.db.Query("SELECT id, time FROM videos")
	if err != nil {
		log.Printf("SQL Query -- %v\n", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		if err = rows.Err(); err != nil {
			log.Printf("SQL List -- %v\n", err)
			return nil, err
		}

		var id, uploadTime string
		var parsedTime time.Time

		if err = rows.Scan(&id, &uploadTime); err != nil {
			log.Printf("SQL Scan -- %v\n", err)
			return nil, err
		}
		if parsedTime, err = time.Parse(layout, uploadTime); err != nil {
			log.Printf("List -- %v", err)
			return nil, err
		}
		metadatas = append(metadatas, VideoMetadata{Id: id, UploadedAt: parsedTime})
	}

	return metadatas, nil
}

func (s *SQLiteVideoMetadataService) Create(videoId string, uploadedAt time.Time) error {
	_, err := s.db.Exec("INSERT INTO videos (id, time) VALUES (?, ?)", videoId, uploadedAt.Format(layout))
	if err != nil {
		log.Printf("SQL Exec -- %v\n", err)
		return err
	}
	log.Println("Successfully inserted video id & time")
	return nil
}

package master

import (
	"sync"
	"time"
)

type FileMetadata struct {
	Path [] string
	Chunks [] string
}

type ChunkMetadata struct {
	ChunkID string
	Version int
	Replicas [] string
	Primary string
	LeaseUntil time.Time
	
}

type MasterState struct {
	mu sync.RWMutex
	Files map[string]*FileMetadata
	Chunks map[string]*ChunkMetadata
	ServerLike []string
}

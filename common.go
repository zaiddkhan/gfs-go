package gfsgo


import "time"

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64
type Checksum int64


/*
 *  ChunkServer
 */

// handshake
type CheckVersionArg struct {
	Handle  ChunkHandle
	Version ChunkVersion
}
type CheckVersionReply struct {
	Stale bool
}

// chunk IO
type ForwardDataArg struct {
	DataID     DataBufferID
	Data       []byte
	ChainOrder []ServerAddress
}
type ForwardDataReply struct {
	ErrorCode ErrorCode
}

type CreateChunkArg struct {
	Handle ChunkHandle
}
type CreateChunkReply struct {
	ErrorCode ErrorCode
}

type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      Offset
	Secondaries []ServerAddress
}
type WriteChunkReply struct {
	ErrorCode ErrorCode
}

type AppendChunkArg struct {
	DataID      DataBufferID
	Secondaries []ServerAddress
}
type AppendChunkReply struct {
	Offset    Offset
	ErrorCode ErrorCode
}

type ApplyMutationArg struct {
	Mtype  MutationType
	DataID DataBufferID
	Offset Offset
}
type ApplyMutationReply struct {
	ErrorCode ErrorCode
}

type PadChunkArg struct {
	Handle ChunkHandle
}
type PadChunkReply struct {
	ErrorCode ErrorCode
}

type ReadChunkArg struct {
	Handle ChunkHandle
	Offset Offset
	Length int
}
type ReadChunkReply struct {
	Data      []byte
	Length    int
	ErrorCode ErrorCode
}

// re-replication
type SendCopyArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type SendCopyReply struct {
	ErrorCode ErrorCode
}

type ApplyCopyArg struct {
	Handle  ChunkHandle
	Data    []byte
	Version ChunkVersion
}
type ApplyCopyReply struct {
	ErrorCode ErrorCode
}

// no use argument
type Nouse struct{}

// handshake
type HeartbeatArg struct {
	Address          ServerAddress // chunkserver address
	LeaseExtensions  []ChunkHandle // leases to be extended
	AbandondedChunks []ChunkHandle // unrecoverable chunks
}
type HeartbeatReply struct {
	Garbage []ChunkHandle
}

type ReportSelfArg struct {
}
type ReportSelfReply struct {
	Chunks []PersistentChunkInfo
}

// chunk info
type GetPrimaryAndSecondariesArg struct {
	Handle ChunkHandle
}
type GetPrimaryAndSecondariesReply struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
}

type ExtendLeaseArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type ExtendLeaseReply struct {
	Expire time.Time
}

type GetReplicasArg struct {
	Handle ChunkHandle
}
type GetReplicasReply struct {
	Locations []ServerAddress
}

type GetFileInfoArg struct {
	Path Path
}
type GetFileInfoReply struct {
	IsDir  bool
	Length int64
	Chunks int64
}

type GetChunkHandleArg struct {
	Path  Path
	Index ChunkIndex
}
type GetChunkHandleReply struct {
	Handle ChunkHandle
}

// namespace operation
type CreateFileArg struct {
	Path Path
}
type CreateFileReply struct{}

type DeleteFileArg struct {
	Path Path
}
type DeleteFileReply struct{}

type RenameFileArg struct {
	Source Path
	Target Path
}
type RenameFileReply struct{}

type MkdirArg struct {
	Path Path
}
type MkdirReply struct{}

type ListArg struct {
	Path Path
}
type ListReply struct {
	Files []PathInfo
}
type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int
}

type Lease struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
}

type PersistentChunkInfo struct {
	Handle   ChunkHandle
	Length   Offset
	Version  ChunkVersion
	Checksum Checksum
}

type PathInfo struct {
	Name string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

type MutationType int

const (
	MutationWrite = iota
	MutationAppend
	MutationPad
)

type ErrorCode int

const (
	Success = iota
	UnknownError
	Timeout
	AppendExceedChunkSize
	WriteExceedChunkSize
	ReadEOF
	NotAvailableForCopy
)

// extended error type with error code
type Error struct {
	Code ErrorCode
	Err  string
}

func (e Error) Error() string {
	return e.Err
}

var (
	Debug int
)

// system config
const (
	// chunk
	LeaseExpire        = 3 * time.Second //1 * time.Minute
	DefaultNumReplicas = 3
	MinimumNumReplicas = 2
	MaxChunkSize       = 32 << 20 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize      = MaxChunkSize / 4
	DeletedFilePrefix  = "__del__"

	// master
	ServerCheckInterval = 400 * time.Millisecond //
	MasterStoreInterval = 30 * time.Hour         // 30 * time.Minute
	ServerTimeout       = 1 * time.Second

	// chunk server
	HeartbeatInterval    = 200 * time.Millisecond
	MutationWaitTimeout  = 4 * time.Second
	ServerStoreInterval  = 40 * time.Hour // 30 * time.Minute
	GarbageCollectionInt = 30 * time.Hour // 1 * time.Day
	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 30 * time.Second

	// client
	ClientTryTimeout = 2*LeaseExpire + 3*ServerTimeout
	LeaseBufferTick  = 500 * time.Millisecond
)
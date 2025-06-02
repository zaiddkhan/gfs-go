package master

import (
	"encoding/gob"
	gfsgo "gfs-go"
	"gfs-go/util"
	"log"
	"net"
	"net/rpc"
	"os"
	"path"
	"time"
)

type Master struct{
	address gfsgo.ServerAddress
	serverRoot string
	l net.Listener
	shutdown chan struct {}
	dead bool
	nm *namespaceManager
	cm *ChunkManager
	csm *chunkServerManager
}

const (
	MetaFileName = "gfs-master.meta"
	FilePerm = 0755
)

func NewAndServe(address gfsgo.ServerAddress,serverRoot string) *Master {
	m := &Master {
		address : address,
		serverRoot: serverRoot,
		shutdown: make(chan struct{}),
	}
	
	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l,e := net.Listen("tcp",string(m.address))
	if e != nil {
		log.Fatal("listen error",e)
	}
	m.l = l
	m.initMetadata()
	
	go func() {
		for {
			select {
				case <- m.shutdown:
				    return
			  
			}
		}
	}()
	return m
	
}

func (m *Master) initMetadata() {
	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()
	m.loadMetadata()
	return 
}


type PersistentBlock struct {
	NamespaceTree []serialTreeNode
	ChunkInfo []serialChunkInfo
}

func (m *Master) loadMetadata() error {
	filename := path.Join(m.serverRoot,MetaFileName)
	file,err := os.OpenFile(filename,os.O_RDONLY,FilePerm)
	if err != nil {
		return err
	}
	
	defer file.Close()
	
	var meta PersistentBlock
	dec := gob.NewDecoder(file)
	err = dec.Decode(&meta)
	
	if err != nil {
		return err
	}
	
	m.nm.Deserialize(meta.NamespaceTree)
	m.cm.Deserialize(meta.ChunkInfo)

	return nil
}

func (m *Master) storeMeta() error {
	filename := path.Join(m.serverRoot,MetaFileName)
	file,err := os.OpenFile(filename,os.O_WRONLY | os.O_CREATE,FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	var meta PersistentBlock
	meta.NamespaceTree = m.nm.Serialize()
	meta.ChunkInfo = m.cm.Serialize()
	
	enc := gob.NewEncoder(file)
	err = enc.Encode(meta)
	return err
}


func (m *Master) serverCheck() error {
	addrs := m.csm.DetectDeadServers()
	
	for _,v := range addrs {
		handles, err := m.csm.RemoveServer(v)
		if err != nil {
			return err
		}
		err = m.cm.RemoveChunks(handles,v)
		if err != nil {
			return err
		}
	}
	handles := m.cm.GetNeedList()
	if handles != nil {
		m.cm.RLock()
		for i:= 0;i<len(handles);i++ {
			ck := m.cm.chunk[handles[i]]
			if ck.expire.Before(time.Now()) {
				ck.Lock()
				m.reReplication(handles[i])
				ck.Unlock()
			}
		}
		m.cm.Unlock()
	}
	return nil
}

func (m *Master) reReplication(handle gfsgo.ChunkHandle) error {
	from,to,err := m.csm.ChooseReplication(handle)
	
	if err != nil {
		return err
	}
	
	var cr gfsgo.CreateChunkReply
	
	err = util.Call(to,"ChunkServer.RPCCreateChunk",gfsgo.CreateChunkArg{handle},&cr)
    if err != nil {
        return err
    }
    var sr gfsgo.SendCopyReply
    err = util.Call(from,"ChunkServer.RPCSendCopy",gfsgo.SendCopyArg{handle,to},&sr)
    if err != nil {
        return err
    }
    m.cm.RegisterReplicas(handle,to,false)
    m.csm.AddChunk([]gfsgo.ServerAddress{to}, handle)
    return nil
}


func (m *Master ) RPCHeartbeat(args gfsgo.HeartbeatArg, reply *gfsgo.HeartbeatReply) error {
	isFirst := m.csm.Heartbeat(args.Address,reply)
	for _, handle := range args.LeaseExtensions {
		continue
		m.cm.ExtendLease(handle,args.Address)
	}
	
	if isFirst {
		var r gfsgo.ReportSelfReply
		err := util.Call(args.Address,"ChunkServer.RPCReportSelf",gfsgo.ReportSelfArg{},&r)
		if err != nil {
			return err
		}
		
		for _, v := range r.Chunks {
			m.cm.RUnlock()
			ck,ok := m.cm.chunk[v.Handle]
			
			if !ok {
				continue
			}
			
			version := ck.version
			m.cm.RUnlock()
			if v.Version == version {
				m.cm.RegisterReplicas(v.Handle, args.Address, true)
				m.csm.AddChunk([]gfsgo.ServerAddress{args.Address},v.Handle)
			}else{
				
			}
		}
	}
	
	return nil
}
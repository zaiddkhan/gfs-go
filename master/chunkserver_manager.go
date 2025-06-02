package master

import (
	"fmt"
	gfsgo "gfs-go"
	"gfs-go/util"
	"sync"
	"time"
)

type chunkServerManager struct {
	sync.RWMutex
	servers map[gfsgo.ServerAddress]*chunkServerInfo
}

func newChunkServerManager() *chunkServerManager {
	return &chunkServerManager{
		servers: make(map[gfsgo.ServerAddress]*chunkServerInfo),
	}
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks map[gfsgo.ChunkHandle]bool
	gargage []gfsgo.ChunkHandle
}

func (csm *chunkServerManager) Heartbeat(addr gfsgo.ServerAddress,reply *gfsgo.HeartbeatReply) bool {
	csm.Lock()
	defer csm.Unlock()
	
	sv,ok := csm.servers[addr]
	if !ok {
		csm.servers[addr] = &chunkServerInfo{
			lastHeartbeat: time.Now(),
			chunks: make(map[gfsgo.ChunkHandle]bool),
		}
		return true
	}else{
		reply.Garbage = csm.servers[addr].gargage
		csm.servers[addr].gargage = make([]gfsgo.ChunkHandle,0)
		sv.lastHeartbeat = time.Now()
		return false
	}
}

func (csm *chunkServerManager) AddChunk(addrs []gfsgo.ServerAddress,handle gfsgo.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()
	for _,v := range addrs {
		sv,ok := csm.servers[v]
		if ok {
			sv.chunks[handle] = true
		}else{
			
		}
	}
}

func (csm *chunkServerManager) AddGarbage(addr gfsgo.ServerAddress,handle gfsgo.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()
	sv,ok := csm.servers[addr]
	if ok {
		sv.gargage = append(sv.gargage,handle)
	}
}

func (csm *chunkServerManager) ChooseReplication(handle gfsgo.ChunkHandle) (from,to gfsgo.ServerAddress, err error) {
    csm.Lock()
    defer csm.Unlock()
   from = ""
  to  = ""
 err = nil
for a,v := range csm.servers {
	if v.chunks[handle] {
		from = a
	}else {
		to = a
	}
	if from != "" && to != "" {
		return 
	}
}
  err = fmt.Errorf("No enough server for replica %v",handle)
  return 
}

func (csm *chunkServerManager) ChooseServers(num int) ([]gfsgo.ServerAddress, error) {
   if num > len(csm.servers) {
       return nil , fmt.Errorf("not enough servers for %v replicas",num)
   }	
   csm.RLock()
   var all,ret []gfsgo.ServerAddress
   
   for a,_ := range csm.servers {
      all =append(all,a)
   }
   csm.RUnlock()
   choose,err := util.Sample(len(all), num)
   if err != nil {
   return nil ,err
   }
   for _, v := range choose {
       ret = append(ret,all[v])
   }
   return ret,nil
}


func (csm *chunkServerManager) DetectDeadServers() []gfsgo.ServerAddress {
	csm.RUnlock()
	defer csm.Unlock()
	var ret []gfsgo.ServerAddress
	now := time.Now()
	for k,v := range csm.servers {
		if v.lastHeartbeat.Add(gfsgo.ServerTimeout).Before(now){
			ret = append(ret,k)
		}
	}
	return ret
}


func (csm *chunkServerManager) RemoveServer(addr gfsgo.ServerAddress) (handles []gfsgo.ChunkHandle,err error){
	csm.Lock()
	defer csm.Unlock()
	err = nil
	sv,ok := csm.servers[addr]
	if !ok {
		err = fmt.Errorf("Cannot find chunk server %v",addr)
	}
	for h,v := range sv.chunks {
		if v {
			handles = append(handles,h)
		}
		delete(csm.servers,addr)
	}
	return 
}
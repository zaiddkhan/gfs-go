package master

import (
	"fmt"
	gfsgo "gfs-go"
	"gfs-go/util"
	"sort"
	"sync"
	"time"

	"golang.org/x/tools/go/analysis/passes/defers"
)



type ChunkManager struct {
	sync.RWMutex
	chunk map[gfsgo.ChunkHandle]*chunkInfo
	file map[gfsgo.Path]*fileInfo
	replicasNeedList []gfsgo.ChunkHandle
	numChunkHandle gfsgo.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location []gfsgo.ServerAddress
	primary gfsgo.ServerAddress
	expire time.Time
	version gfsgo.ChunkVersion
	checksum gfsgo.Checksum
	path gfsgo.Path
}

type fileInfo struct {
	sync.RWMutex
	handles []gfsgo.ChunkHandle
	
}

type serialChunkInfo struct {
	Path gfsgo.Path
	Info []gfsgo.PersistentChunkInfo
}

func (cm *ChunkManager) Deserialize(files []serialChunkInfo) error {
	cm.Lock()
	defer cm.Unlock()
	
	now := time.Now()
	for _, v := range files {
		f := new(fileInfo)
		for _, ck := range v.Info {
			f.handles = append(f.handles,ck.Handle)
			cm.chunk[ck.Handle] = &chunkInfo{
				expire : now,
				version : ck.Version,
				checksum: ck.Checksum,
			}
		}
		cm.numChunkHandle += gfsgo.ChunkHandle(len(v.Info))
		cm.file[v.Path] = f
	}
	return nil
}

func (cm *ChunkManager) Serialize() []serialChunkInfo {
	cm.RLock()
	defer cm.RUnlock()
	
	var ret []serialChunkInfo
	for k,v := range cm.file {
		var chunks []gfsgo.PersistentChunkInfo
		for _,handle := range v.handles {
			chunks = append(chunks,gfsgo.PersistentChunkInfo{
				Handle : handle,
				Length : 0,
				Version : cm.chunk[handle].version,
				Checksum: 0,
			})
		}
		ret = append(ret,serialChunkInfo{
			Path: k,
			Info: chunks,
		})
	}
	return ret
}

func newChunkManager() *ChunkManager {
	cm := &ChunkManager {
		chunk : make(map[gfsgo.ChunkHandle]*chunkInfo),
		file : make(map[gfsgo.Path]*fileInfo),
	}
	return cm
}


func (cm *ChunkManager) RegisterReplicas(handle gfsgo.ChunkHandle, addr gfsgo.ServerAddress,useLock bool) error{
	var ck *chunkInfo
	var ok bool
	
	if useLock {
		cm.RLock()
		ck, ok = cm.chunk[handle]
		cm.RUnlock()
		ck.Lock()
		defer ck.Unlock()
	} else{
		ck,ok = cm.chunk[handle]
	}
	
	if !ok {
		return  fmt.Errorf("cannot find chunk %v",handle)
	}
	
	ck.location = append(ck.location,addr)
	return nil

}

func (cm *ChunkManager) GetReplicas(handle gfsgo.ChunkHandle) ([]gfsgo.ServerAddress,error) {
    cm.RLock()
     ck, ok := cm.chunk[handle]
      cm.RUnlock()
	  if !ok {
	      return nil,fmt.Errorf("Cannot find chunk %v", handle)
	  }	
	  return ck.location,nil
}

func (cm *ChunkManager) GetChunk(path gfsgo.Path,index gfsgo.ChunkIndex) (gfsgo.ChunkHandle,error){
	cm.RLock()
	defer cm.RUnlock()
	fileInfo, ok := cm.file[path]
	
	if !ok {
		return 1, fmt.Errorf("cannot get handle")
		
	}
	
	if index < 0 || int(index) >= len(fileInfo.handles) {
		return  -1,fmt.Errorf("Invalid index", )
	}
	return fileInfo.handles[index],nil
}

func (cm *ChunkManager) GetLeaseHolder(handle gfsgo.ChunkHandle) (*gfsgo.Lease, []gfsgo.ServerAddress,error) {
	cm.RLock()
	ck,ok := cm.chunk[handle]
	cm.RUnlock()
	
	if !ok {
		return nil,nil, fmt.Errorf("invalid chunk handle")
	}
	
	ck.Lock()
	defer ck.Unlock()
	
	var stableServers []gfsgo.ServerAddress
	
	ret := &gfsgo.Lease{}
	
	if ck.expire.Before(time.Now()) {
		ck.version++
		arg := gfsgo.CheckVersionArg{handle,ck.version}
		var newList []string
		var lock sync.Mutex
		
		var wg sync.WaitGroup
		wg.Add(len(ck.location))
		for _,v := range ck.location {
			go func(addr gfsgo.ServerAddress) {
				var r gfsgo.CheckVersionReply
				
				
				err := util.Call(addr,"ChunkServer.RPCCheckVersion",arg,&r)
				
				if err == nil && r.Stale == false {
					lock.Lock()
					newList = append(newList,string(addr))
					lock.Unlock()
				}else{
					stableServers = append(stableServers,addr)
				}
				wg.Done()
			}(v)
		}
		wg.Wait()
		
		ck.location = make([]gfsgo.ServerAddress,len(newList))
		
		for i := range newList {
			ck.location[i] = gfsgo.ServerAddress(newList[i])
			
		}
		
		if len(ck.location) < gfsgo.MinimumNumReplicas {
			cm.Lock()
			cm.replicasNeedList = append(cm.replicasNeedList,handle)
			cm.Unlock()
			
			if len(ck.location) == 0 {
				ck.version -- 
				return nil,nil, fmt.Errorf("no replicas of %v",handle)
			}
		}
		ck.primary = ck.location[0]
		ck.expire = time.Now().Add(gfsgo.LeaseExpire)
		
	}
	ret.Primary = ck.primary
    ret.Expire = ck.expire
    for _, v := range ck.location {
         if v != ck.primary {
               ret.Secondaries = append(ret.Secondaries,v)
         }
    }	
    return ret,stableServers,nil
	
}


func (cm *ChunkManager) ExtendLease(handle gfsgo.ChunkHandle,primary gfsgo.ServerAddress) error {
	cm.RLock()
	ck,ok := cm.chunk[handle]
	cm.RUnlock()
	defer ck.Unlock()
	
	if !ok {
		return fmt.Errorf("invalid chunk handle")
	}
	
	now := time.Now()
	
	if ck.primary != primary && ck.expire.After(now) {
		return fmt.Errorf("does not hold any lease ")
	}
	
	ck.primary = primary
	ck.expire = now.Add(gfsgo.LeaseExpire)
	return nil
}

func (cm *ChunkManager) CreateChunk(path gfsgo.Path,addrs []gfsgo.ServerAddress) (gfsgo.ChunkHandle,[]gfsgo.ServerAddress,error) {
	cm.Lock()
	defer cm.Unlock()
	
	handle := cm.numChunkHandle
	cm.numChunkHandle++
	
	fileinfo,ok := cm.file[path]
	
	if !ok {
		fileinfo = new(fileInfo)
		cm.file[path] = fileinfo
	}
	
	fileinfo.handles = append(fileinfo.handles,handle)
	
	ck := &chunkInfo{
		path : path,
	}
	cm.chunk[handle] = ck
	
	var errList string
	var success []gfsgo.ServerAddress
	for _, v := range addrs {
		var r gfsgo.CreateChunkReply
		
		err := util.Call(v,"ChunkServer.RPCCreateChunk",  gfsgo.CreateChunkArg{handle},&r)
		
		if err == nil {
			ck.location = append(ck.location,v)
			success = append(success,v)
		}else{
			errList += err.Error() + ";"
		}
	}
	
	if errList == "" {
		return handle,success,nil
	} else {
		cm.replicasNeedList = append(cm.replicasNeedList, handle)
		return handle,success,fmt.Errorf(errList)
	}
	
	
}

func (cm *ChunkManager) RemoveChunks(handles []gfsgo.ChunkHandle,server gfsgo.ServerAddress) error {
    errList := ""
   for _,v := range handles {
      cm.RLock()
      ck,ok := cm.chunk[v]
      cm.RUnlock()
      
      if !ok {
         continue
      }
      
      ck.Lock()
      var newList []gfsgo.ServerAddress
      for i := range ck.location {
          if ck.location[i] != server {
              newList = append(newList,ck.location[i])
          }
      }
      
      ck.location = newList
      ck.expire = time.Now()
      num := len(ck.location)
      ck.Unlock()
      
      if num < gfsgo.MinimumNumReplicas {
         cm.replicasNeedList = append(cm.replicasNeedList,v)
         if num == 0 {
             errList += fmt.Sprintf("Chunk %d has no replicas", v) + ";"
         }
      }
   }
  if errList == "" {
       return nil
  }	else {
      return  fmt.Errorf(errList)
  }
}

func (cm *ChunkManager) GetNeedList() []gfsgo.ChunkHandle {
	cm.Lock()
	defer cm.Unlock()
	
	var newList []int
	for _,v := range cm.replicasNeedList {
		
		if len(cm.chunk[v].location) < gfsgo.MinimumNumReplicas {
			newList = append(newList,int(v))
		}
		
	}
	
	sort.Ints(newList)
	cm.replicasNeedList = make([] gfsgo.ChunkHandle,0)
	for i,v := range newList {
		if i == 0 || v != newList[i-1] {
			cm.replicasNeedList = append(cm.replicasNeedList,gfsgo.ChunkHandle(v))
		}
	}
	if(len(cm.replicasNeedList) > 0) {
		return cm.replicasNeedList
	} else {
		return nil
	}
}
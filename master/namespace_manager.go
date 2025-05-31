package master

import (
	"fmt"
	"gfs-go"
	"log"
	"strings"
	"sync"
)

type namespaceManager struct {
	root *nsTree
	serialCt int
}

type nsTree struct {
	sync.RWMutex
	
	isDir bool
	children map[string]*nsTree
	length int64
	chunks int64
}

type serialTreeNode struct {
	isDir bool
	children map[string]int
	chunks int64
}

func (nm *namespaceManager) tree2array(array *[]serialTreeNode,node *nsTree) int {

	n := serialTreeNode{isDir: node.isDir,chunks: node.chunks}
	if node.isDir {
		n.children = make(map[string]int)
		for k,v := range node.children {
			n.children[k] = nm.tree2array(array,v)
		}
	}
	*array = append(*array,n)
	ret := nm.serialCt
	nm.serialCt++
	return ret

}

func (nm *namespaceManager) Serialize() []serialTreeNode {
	nm.root.RLock();
	defer nm.root.RUnlock()
	nm.serialCt = 0
	var ret []serialTreeNode
	nm.tree2array(&ret,nm.root)
	return ret
}

func (nm *namespaceManager) array2tree(array []serialTreeNode,id int) *nsTree {
	n := &nsTree{
		isDir: array[id].isDir,
		chunks: array[id].chunks,
	}
	if array[id].isDir {
		n.children = make(map[string]*nsTree)
		for k,v := range array[id].children {
			n.children[k] = nm.array2tree(array,v)
		}
	}
	return n
}

func (nm *namespaceManager) Deserialize(array []serialTreeNode) error {
	nm.root.Lock();
	defer nm.root.Unlock();
	nm.root = nm.array2tree(array,len(array) - 1)
	return nil
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root : &nsTree{
			isDir: true,
			children: make(map[string]*nsTree),
		},
	}
	
	return nm
}

func (nm *namespaceManager) lockParents(p gfsgo.Path,goDown bool) ([]string,*nsTree,error){
	ps := strings.Split(string(p),"/")[1:]
	cwd := nm.root
	if len(ps) > 0 {
		cwd.RLock()
		for i,name := range ps[:len(ps)] {
			c , ok := cwd.children[name]
			if !ok {
				return ps,cwd,fmt.Errorf("path %s is not found",p)
			}
			if i == len(ps) - 1 {
				if goDown {
					cwd = c
				}
			} else{
				cwd = c
				cwd.RLock()
			}
		}
	}
	return ps,cwd,nil
}

func (nm *namespaceManager) unlockParents(ps [] string){
	cwd := nm.root;
	if len(ps) > 0 {
		cwd.Unlock()
		for _,name := range ps[:len(ps)- 1]{
			c,ok := cwd.children[name]
			if !ok {
				log.Fatal("error while unlocking")
				return 
			}
			cwd = c
			cwd.RUnlock()
		}
	}
}

func (nm *namespaceManager) PartitionLastName(p gfsgo.Path) (gfsgo.Path,string) {
	for i := len(p) - 1;i>=0;i-- {
		if p[i] == '/'{
			return p[:i], string(p[i+1:])
		}
	}
	return "",""
}

func (nm *namespaceManager) Delete(p gfsgo.Path) error {
	ps,cwd,err := nm.lockParents(p,false)
	if err != nil {
		return err
	}
	filename := ps[len(ps)-1]
	cwd.Lock()
	defer cwd.Unlock()
	node := cwd.children[filename]
	delete(cwd.children,filename)
	cwd.children["deleted"+filename] = node
	return nil
}

func (nm *namespaceManager) Rename(source,target gfsgo.Path) error {
	log.Fatal("unsupported rename")
	return nil
}

func (nm *namespaceManager) Mkdir(p gfsgo.Path) error { 
	var filename string
	p,filename = nm.PartitionLastName(p)
	ps,cwd,err := nm.lockParents(p,true)
	defer nm.unlockParents(ps)
	if err != nil {
		return err
	}
	cwd.Lock()
	defer cwd.Unlock()
	if _,ok := cwd.children[filename]; ok {
		return fmt.Errorf("path %s already exists",p)
	}
	cwd.children[filename] = &nsTree{
		isDir: true,
		children: make(map[string]*nsTree),
	}
	return nil
}

func (nm *namespaceManager) List(p gfsgo.Path) ([]gfsgo.PathInfo,error) {

	var dir *nsTree
	if p == gfsgo.Path("/") {
		dir = nm.root
	} else {
		ps,cwd,err := nm.lockParents(p,true)
		defer nm.unlockParents(ps)
		if err != nil {
			return nil,err
		}
		dir = cwd
		
	}
	dir.RLock()
	defer dir.RUnlock()
	
	if !dir.isDir {
		return  nil, fmt.Errorf("path %s is a file, not a directory", p)
	}
	ls := make([]gfsgo.PathInfo,0,len(dir.children))
	for name,v := range dir.children {
		ls = append(ls,gfsgo.PathInfo{
			Name : name,
			IsDir: v.isDir,
            Length: v.length,
            Chunks: v.chunks,
		})
	}
	return ls,nil
}
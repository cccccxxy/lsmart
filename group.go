package lsmart
import (
	"bytes"
	"sort"
	"sync"
)

// Group 表示一个分组，包含多个SST文件
type Group struct {
	id        int           // 分组ID
	conf      *Config       // 配置文件
	nodes     []*Node       // 分组内的SST文件节点
	startKey  []byte        // 分组的最小key
	endKey    []byte        // 分组的最大key
	size      uint64        // 分组总大小
	nodeLock  sync.RWMutex  // 节点读写锁
}

// NewGroup 创建新的分组
func NewGroup(id int, conf *Config) *Group {
	return &Group{
		id:    id,
		conf:  conf,
		nodes: make([]*Node, 0, conf.GroupSize),
	}
}

// AddNode 向分组添加SST节点
func (g *Group) AddNode(node *Node) {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()

	g.nodes = append(g.nodes, node)
	g.size += node.size

	// 更新分组的key范围
	g.updateKeyRange()
	
	// 保持节点按key范围排序
	g.sortNodes()
}

// RemoveNode 从分组移除SST节点
func (g *Group) RemoveNode(node *Node) bool {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()

	for i, n := range g.nodes {
		if n == node {
			g.nodes = append(g.nodes[:i], g.nodes[i+1:]...)
			g.size -= node.size
			g.updateKeyRange()
			return true
		}
	}
	return false
}

// Get 从分组中查找key对应的value
func (g *Group) Get(key []byte) ([]byte, bool, error) {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()

	// 检查key是否在分组范围内
	if !g.keyInRange(key) {
		return nil, false, nil
	}

	// 按照从新到旧的顺序查找（倒序遍历）
	for i := len(g.nodes) - 1; i >= 0; i-- {
		if value, ok, err := g.nodes[i].Get(key); err != nil {
			return nil, false, err
		} else if ok {
			return value, true, nil
		}
	}

	return nil, false, nil
}

// GetAllKVs 获取分组内所有KV数据（去重并排序）
func (g *Group) GetAllKVs() ([]*KV, error) {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()

	// 使用临时memtable来处理重复key并排序
	tempMemTable := g.conf.MemTableConstructor()
	// 按照从旧到新的顺序添加数据，新数据会覆盖旧数据
	for _, node := range g.nodes {
		kvs, err := node.GetAll()
		if err != nil {
			return nil, err
		}
		
		for _, kv := range kvs {
			tempMemTable.Put(kv.Key, kv.Value)
		}
	}

	// 获取排序后的结果
	memKVs := tempMemTable.All()
	result := make([]*KV, 0, len(memKVs))
	for _, kv := range memKVs {
		result = append(result, &KV{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	return result, nil
}

// IsFull 检查分组是否已满
func (g *Group) IsFull() bool {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	return len(g.nodes) >= g.conf.GroupSize
}

// ShouldCompact 检查分组是否需要压缩
func (g *Group) ShouldCompact() bool {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	
	threshold := float64(g.conf.GroupSize) * g.conf.CompactionRatio
	return float64(len(g.nodes)) >= threshold
}

// Size 获取分组总大小
func (g *Group) Size() uint64 {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	return g.size
}

// NodeCount 获取分组内节点数量
func (g *Group) NodeCount() int {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	return len(g.nodes)
}

// StartKey 获取分组最小key
func (g *Group) StartKey() []byte {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	return g.startKey
}

// EndKey 获取分组最大key
func (g *Group) EndKey() []byte {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	return g.endKey
}

// ID 获取分组ID
func (g *Group) ID() int {
	return g.id
}

// Close 关闭分组，释放资源
func (g *Group) Close() {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()
	
	for _, node := range g.nodes {
		node.Close()
	}
}

// Destroy 销毁分组，删除所有SST文件
func (g *Group) Destroy() {
	g.nodeLock.Lock()
	defer g.nodeLock.Unlock()
	
	for _, node := range g.nodes {
		node.Destroy()
	}
}

// 更新分组的key范围
func (g *Group) updateKeyRange() {
	if len(g.nodes) == 0 {
		g.startKey = nil
		g.endKey = nil
		return
	}

	g.startKey = g.nodes[0].Start()
	g.endKey = g.nodes[0].End()

	for _, node := range g.nodes {
		if bytes.Compare(node.Start(), g.startKey) < 0 {
			g.startKey = node.Start()
		}
		if bytes.Compare(node.End(), g.endKey) > 0 {
			g.endKey = node.End()
		}
	}
}

// 对节点按key范围排序
func (g *Group) sortNodes() {
	sort.Slice(g.nodes, func(i, j int) bool {
		return bytes.Compare(g.nodes[i].Start(), g.nodes[j].Start()) < 0
	})
}

// 检查key是否在分组范围内
func (g *Group) keyInRange(key []byte) bool {
	if g.startKey == nil || g.endKey == nil {
		return false
	}
	return bytes.Compare(key, g.startKey) >= 0 && bytes.Compare(key, g.endKey) <= 0
}

// GetNodes 获取分组内所有节点（用于压缩等操作）
func (g *Group) GetNodes() []*Node {
	g.nodeLock.RLock()
	defer g.nodeLock.RUnlock()
	
	nodes := make([]*Node, len(g.nodes))
	copy(nodes, g.nodes)
	return nodes
}

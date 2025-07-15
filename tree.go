package lsmart

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/cccccxxy/lsmart/memtable"
	"github.com/cccccxxy/lsmart/wal"
)

// memTableCompactItem 内存表压缩项
type memTableCompactItem struct {
	walFile  string
	memTable memtable.MemTable
}

// Tree 单层分组LSM-tree结构
// 1 构造一棵树，基于 config 与磁盘文件映射
// 2 写入一笔数据
// 3 查询一笔数据
type Tree struct {
	conf *Config

	// 读写数据时使用的锁
	dataLock sync.RWMutex

	// 分组读写锁
	groupLock sync.RWMutex

	// 读写 memtable
	memTable memtable.MemTable

	// 只读 memtable
	rOnlyMemTable []*memTableCompactItem

	// 预写日志写入口
	walWriter *wal.WALWriter

	// 单层分组结构（替代原来的多层nodes）
	groups []*Group

	// memtable 达到阈值时，通过该 chan 传递信号，进行溢写工作
	memCompactC chan *memTableCompactItem

	// 分组压缩信号通道
	groupCompactC chan int

	// lsm tree 停止时通过该 chan 传递信号
	stopc chan struct{}

	// memtable index，需要与 wal 文件一一对应
	memTableIndex int

	// 分组seq生成器
	groupSeq atomic.Int32

	// SST文件seq生成器
	sstSeq atomic.Int32
}

// NewTree 构建出一棵 lsm tree
func NewTree(conf *Config) (*Tree, error) {
	// 1 构造 lsm tree 实例
	t := Tree{
		conf:          conf,
		memCompactC:   make(chan *memTableCompactItem),
		groupCompactC: make(chan int),
		stopc:         make(chan struct{}),
		groups:        make([]*Group, 0, conf.MaxGroups),
	}

	// 2 读取 sst 文件，还原出整棵树
	if err := t.constructTree(); err != nil {
		return nil, err
	}

	// 3 运行 lsm tree 压缩调整协程
	go t.compact()

	// 4 读取 wal 还原出 memtable
	if err := t.constructMemtable(); err != nil {
		return nil, err
	}

	// 5 返回 lsm tree 实例
	return &t, nil
}

func (t *Tree) Close() {
	close(t.stopc)
	t.groupLock.Lock()
	defer t.groupLock.Unlock()

	for _, group := range t.groups {
		group.Close()
	}
}

// Put 写入一组 kv 对到 lsm tree. 会直接写入到读写 memtable 中.
func (t *Tree) Put(key, value []byte) error {
	// 1 加写锁
	t.dataLock.Lock()
	defer t.dataLock.Unlock()
	// 2 数据预写入预写日志中，防止因宕机引起 memtable 数据丢失.
	if err := t.walWriter.Write(key, value); err != nil {
		return err
	}

	// 3 数据写入读写跳表
	t.memTable.Put(key, value)

	// 4 倘若读写跳表的大小未达到阈值，则直接返回.
	// 考虑到溢写成 sstable 后，需要有一些辅助的元数据，预估容量放大为 5/4 倍
	if uint64(t.memTable.Size()*5/4) <= t.conf.GroupSSTSize {
		return nil
	}

	// 5 倘若读写跳表数据量达到上限，则需要切换跳表
	t.refreshMemTableLocked()
	return nil
}

// Get 根据 key 读取数据
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	t.dataLock.RLock()
	// 1 首先读 active memtable.
	value, ok := t.memTable.Get(key)
	if ok {
		t.dataLock.RUnlock()
		return value, true, nil
	}

	// 2 读 readOnly memtable.  按照 index 倒序遍历，因为 index 越大，数据越晚写入，实时性越强
	for i := len(t.rOnlyMemTable) - 1; i >= 0; i-- {
		value, ok = t.rOnlyMemTable[i].memTable.Get(key)
		if ok {
			t.dataLock.RUnlock()
			return value, true, nil
		}
	}
	t.dataLock.RUnlock()

	// 3 读分组中的SST文件，按照分组倒序遍历（新分组优先）
	t.groupLock.RLock()
	defer t.groupLock.RUnlock()
	for i := len(t.groups) - 1; i >= 0; i-- {
		if value, ok, err := t.groups[i].Get(key); err != nil {
			return nil, false, err
		} else if ok {
			return value, true, nil
		}
	}

	// 4 至此都没有读到数据，则返回 key 不存在.
	return nil, false, nil
}

// 切换读写跳表为只读跳表，并构建新的读写跳表
func (t *Tree) refreshMemTableLocked() {
	// 辞旧
	// 将读写跳表切换为只读跳表，追加到 slice 中，并通过 chan 发送给 compact 协程，由其负责进行溢写成为SST文件的操作.
	oldItem := memTableCompactItem{
		walFile:  t.walFile(),
		memTable: t.memTable,
	}
	t.rOnlyMemTable = append(t.rOnlyMemTable, &oldItem)
	t.walWriter.Close()
	go func() {
		t.memCompactC <- &oldItem
	}()

	// 迎新
	// 构造一个新的读写 memtable，并构造与之相应的 wal 文件.
	t.memTableIndex++
	t.newMemTable()
}

func (t *Tree) newMemTable() {
	t.walWriter, _ = wal.NewWALWriter(t.walFile())
	t.memTable = t.conf.MemTableConstructor()
}

// findOrCreateGroup 查找或创建适合存放指定key范围的分组
func (t *Tree) findOrCreateGroup(startKey, endKey []byte) *Group {
	t.groupLock.Lock()
	defer t.groupLock.Unlock()

	// 查找是否有重叠的分组
	for _, group := range t.groups {
		if group.NodeCount() < t.conf.GroupSize {
			// 检查key范围是否有重叠或者分组为空
			if group.StartKey() == nil || group.EndKey() == nil ||
				!(bytes.Compare(endKey, group.StartKey()) < 0 || bytes.Compare(startKey, group.EndKey()) > 0) {
				return group
			}
		}
	}

	// 没有找到合适的分组，创建新分组
	if len(t.groups) < t.conf.MaxGroups {
		groupID := int(t.groupSeq.Add(1))
		newGroup := NewGroup(groupID, t.conf)
		t.groups = append(t.groups, newGroup)
		return newGroup
	}

	// 如果已达到最大分组数，返回最后一个分组（可能需要压缩）
	if len(t.groups) > 0 {
		return t.groups[len(t.groups)-1]
	}

	// 创建第一个分组
	groupID := int(t.groupSeq.Add(1))
	newGroup := NewGroup(groupID, t.conf)
	t.groups = append(t.groups, newGroup)
	return newGroup
}

// tryTriggerGroupCompact 尝试触发分组压缩
func (t *Tree) tryTriggerGroupCompact(groupID int) {
	t.groupLock.RLock()
	defer t.groupLock.RUnlock()

	for _, group := range t.groups {
		if group.ID() == groupID && group.ShouldCompact() {
			go func() {
				t.groupCompactC <- groupID
			}()
			break
		}
	}
}

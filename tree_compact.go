package lsmart
import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/cccccxxy/lsmart/memtable"
)

// 运行 compact 协程.
func (t *Tree) compact() {
	for {
		select {
		// 接收到 lsm tree 终止信号，退出协程.
		case <-t.stopc:
			return
			// 接收到 read-only memtable，需要将其溢写到磁盘成为SST文件.
		case memCompactItem := <-t.memCompactC:
			t.compactMemTable(memCompactItem)
			// 接收到分组压缩指令，需要执行分组内SST文件合并流程.
		case groupID := <-t.groupCompactC:
			t.compactGroup(groupID)
		}
	}
}

// 针对指定分组进行压缩合并操作
func (t *Tree) compactGroup(groupID int) {
	t.groupLock.Lock()
	
	// 找到目标分组
	var targetGroup *Group
	for _, group := range t.groups {
		if group.ID() == groupID {
			targetGroup = group
			break
		}
	}
	
	if targetGroup == nil {
		t.groupLock.Unlock()
		return
	}
	
	// 获取分组内所有节点
	nodes := targetGroup.GetNodes()
	if len(nodes) <= 1 {
		t.groupLock.Unlock()
		return // 不需要压缩
	}
	
	t.groupLock.Unlock()

	// 获取所有需要合并的KV数据
	allKVs, err := targetGroup.GetAllKVs()
	if err != nil {
		return
	}

	if len(allKVs) == 0 {
		return
	}

	// 创建新的SST文件
	seq := t.sstSeq.Add(1)
	sstWriter, err := NewSSTWriter(t.sstFile(groupID, seq), t.conf)
	if err != nil {
		return
	}
	defer sstWriter.Close()

	// 将所有KV数据写入新的SST文件
	currentSize := uint64(0)
	var newNodes []*Node

	for i, kv := range allKVs {
		// 检查是否需要创建新的SST文件
		if currentSize > 0 && currentSize+uint64(len(kv.Key)+len(kv.Value)) > t.conf.GroupSSTSize {
			// 完成当前SST文件
			size, blockToFilter, index := sstWriter.Finish()
			newNode := t.createNodeFromSST(groupID, seq, size, blockToFilter, index)
			if newNode != nil {
				newNodes = append(newNodes, newNode)
			}
			sstWriter.Close()

			// 创建新的SST文件
			seq = t.sstSeq.Add(1)
			sstWriter, err = NewSSTWriter(t.sstFile(groupID, seq), t.conf)
			if err != nil {
				break
			}
			defer sstWriter.Close()
			currentSize = 0
		}

		// 写入KV数据
		sstWriter.Append(kv.Key, kv.Value)
		currentSize += uint64(len(kv.Key) + len(kv.Value))
		// 如果是最后一条数据，完成SST文件
		if i == len(allKVs)-1 {
			size, blockToFilter, index := sstWriter.Finish()
			newNode := t.createNodeFromSST(groupID, seq, size, blockToFilter, index)
			if newNode != nil {
				newNodes = append(newNodes, newNode)
			}
		}
	}

	// 替换分组中的旧节点
	t.replaceGroupNodes(targetGroup, nodes, newNodes)
}

// 创建新的Node从SST文件
func (t *Tree) createNodeFromSST(groupID int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) *Node {
	fileName := t.sstFile(groupID, seq)
	sstReader, err := NewSSTReader(fileName, t.conf)
	if err != nil {
		return nil
	}
	return NewNode(t.conf, fileName, sstReader, groupID, seq, size, blockToFilter, index)
}

// 替换分组中的节点
func (t *Tree) replaceGroupNodes(group *Group, oldNodes []*Node, newNodes []*Node) {
	// 移除旧节点
	for _, oldNode := range oldNodes {
		group.RemoveNode(oldNode)
	}

	// 添加新节点
	for _, newNode := range newNodes {
		group.AddNode(newNode)
	}

	// 异步销毁旧节点
	go func() {
		for _, oldNode := range oldNodes {
			oldNode.Destroy()
		}
	}()
}

// 将只读 memtable 溢写落盘成为SST文件
func (t *Tree) compactMemTable(memCompactItem *memTableCompactItem) {
	// 处理 memtable 溢写工作:
	// 1 memtable 溢写到SST文件中
	t.flushMemTable(memCompactItem.memTable)
	// 2 从 rOnly slice 中回收对应的 table
	t.dataLock.Lock()
	for i := 0; i < len(t.rOnlyMemTable); i++ {
		if t.rOnlyMemTable[i].memTable != memCompactItem.memTable {
			continue
		}
		t.rOnlyMemTable = append(t.rOnlyMemTable[:i], t.rOnlyMemTable[i+1:]...)
		break
	}
	t.dataLock.Unlock()
	// 3 删除相应的预写日志. 因为 memtable 落盘后数据已经安全，不存在丢失风险
	_ = os.Remove(memCompactItem.walFile)
}

// 将 memtable 的数据溢写落盘成为一个新的 SST 文件
func (t *Tree) flushMemTable(memTable memtable.MemTable) {
	allKVs := memTable.All()
	if len(allKVs) == 0 {
		return
	}
	// 确定key范围
	startKey := allKVs[0].Key
	endKey := allKVs[len(allKVs)-1].Key

	// 找到或创建合适的分组
	group := t.findOrCreateGroup(startKey, endKey)

	// 创建新的SST文件
	seq := t.sstSeq.Add(1)
	sstWriter, err := NewSSTWriter(t.sstFile(group.ID(), seq), t.conf)
	if err != nil {
		return
	}
	defer sstWriter.Close()
	// 遍历 memtable 写入数据到 sst writer
	for _, kv := range allKVs {
		sstWriter.Append(kv.Key, kv.Value)
	}

	// sstable 落盘
	size, blockToFilter, index := sstWriter.Finish()

	// 创建新节点并添加到分组
	newNode := t.createNodeFromSST(group.ID(), seq, size, blockToFilter, index)
	if newNode != nil {
		group.AddNode(newNode)
		
		// 尝试触发分组压缩
		t.tryTriggerGroupCompact(group.ID())
	}
}

// 生成SST文件名
func (t *Tree) sstFile(groupID int, seq int32) string {
	return fmt.Sprintf("g%d_%d.sst", groupID, seq)
}
func (t *Tree) walFile() string {
	return path.Join(t.conf.Dir, "walfile", fmt.Sprintf("%d.wal", t.memTableIndex))
}

func walFileToMemTableIndex(walFile string) int {
	rawIndex := strings.Replace(walFile, ".wal", "", -1)
	index, _ := strconv.Atoi(rawIndex)
	return index
}
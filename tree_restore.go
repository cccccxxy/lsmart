package lsmart
import (
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/cccccxxy/lsmart/wal"
)

// 读取 sst 文件，还原出整棵树的分组结构
func (t *Tree) constructTree() error {
	// 读取 sst 文件目录下的 sst 文件列表
	sstEntries, err := t.getSortedSSTEntries()
	if err != nil {
		return err
	}

	// 按分组组织SST文件
	groupFiles := make(map[int][]fs.DirEntry)
	maxGroupID := 0
	maxSSTSeq := int32(0)
	// 遍历每个 sst 文件，按分组分类
	for _, sstEntry := range sstEntries {
		groupID, seq := getGroupSeqFromSSTFile(sstEntry.Name())
		
		if groupID > maxGroupID {
			maxGroupID = groupID
		}
		if seq > maxSSTSeq {
			maxSSTSeq = seq
		}
		
		groupFiles[groupID] = append(groupFiles[groupID], sstEntry)
	}

	// 设置序列号生成器
	t.groupSeq.Store(int32(maxGroupID))
	t.sstSeq.Store(maxSSTSeq)

	// 为每个分组创建Group并加载SST文件
	for groupID, files := range groupFiles {
		group := NewGroup(groupID, t.conf)
		
		// 加载分组内的所有SST文件
		for _, sstEntry := range files {
			node, err := t.loadNode(sstEntry, groupID)
			if err != nil {
				return err
			}
			group.AddNode(node)
		}
		
		t.groups = append(t.groups, group)
	}

	// 按分组ID排序
	sort.Slice(t.groups, func(i, j int) bool {
		return t.groups[i].ID() < t.groups[j].ID()
	})

	return nil
}

func (t *Tree) getSortedSSTEntries() ([]fs.DirEntry, error) {
	allEntries, err := os.ReadDir(t.conf.Dir)
	if err != nil {
		return nil, err
	}

	sstEntries := make([]fs.DirEntry, 0, len(allEntries))
	for _, entry := range allEntries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}

		sstEntries = append(sstEntries, entry)
	}
	// 按分组ID和序列号排序
	sort.Slice(sstEntries, func(i, j int) bool {
		groupI, seqI := getGroupSeqFromSSTFile(sstEntries[i].Name())
		groupJ, seqJ := getGroupSeqFromSSTFile(sstEntries[j].Name())
		if groupI == groupJ {
			return seqI < seqJ
		}
		return groupI < groupJ
	})
	return sstEntries, nil
}

// 将一个 sst 文件作为一个 node 加载
func (t *Tree) loadNode(sstEntry fs.DirEntry, groupID int) (*Node, error) {
	// 创建 sst 文件对应的 reader
	sstReader, err := NewSSTReader(sstEntry.Name(), t.conf)
	if err != nil {
		return nil, err
	}

	// 读取各 block 块对应的 filter 信息
	blockToFilter, err := sstReader.ReadFilter()
	if err != nil {
		sstReader.Close()
		return nil, err
	}

	// 读取 index 信息
	index, err := sstReader.ReadIndex()
	if err != nil {
		sstReader.Close()
		return nil, err
	}

	// 获取 sst 文件的大小，单位 byte
	size, err := sstReader.Size()
	if err != nil {
		sstReader.Close()
		return nil, err
	}
	// 解析 sst 文件名，得知 sst 文件对应的分组ID以及 seq 号
	_, seq := getGroupSeqFromSSTFile(sstEntry.Name())
	
	// 创建节点
	node := NewNode(t.conf, sstEntry.Name(), sstReader, groupID, seq, size, blockToFilter, index)
	return node, nil
}

func getGroupSeqFromSSTFile(file string) (groupID int, seq int32) {
	file = strings.Replace(file, ".sst", "", -1)
	splitted := strings.Split(file, "_")
	groupID, _ = strconv.Atoi(splitted[0])
	_seq, _ := strconv.Atoi(splitted[1])
	return groupID, int32(_seq)
}
// 读取 wal 还原出 memtable
func (t *Tree) constructMemtable() error {
	// 1 读 wal 目录，获取所有的 wal 文件
	raw, err := os.ReadDir(path.Join(t.conf.Dir, "walfile"))
	if err != nil {
		// 如果wal目录不存在，创建新的memtable
		t.newMemTable()
		return nil
	}

	// 2 wal 文件除杂
	var wals []fs.DirEntry
	for _, entry := range raw {
		if entry.IsDir() {
			continue
		}

		// 要求文件必须为 .wal 类型
		if !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}

		wals = append(wals, entry)
	}

	// 3 倘若 wal 目录不存在或者 wal 文件不存在，则构造一个新的 memtable
	if len(wals) == 0 {
		t.newMemTable()
		return nil
	}

	// 4 依次还原 memtable. 最晚一个 memtable 作为读写 memtable
	// 前置 memtable 作为只读 memtable，分别添加到内存 slice 和 channel 中.
	return t.restoreMemTable(wals)
}

// 基于 wal 文件还原出一系列只读 memtable 和唯一一个读写 memtable
func (t *Tree) restoreMemTable(wals []fs.DirEntry) error {
	// 1 wal 排序，index 单调递增，数据实时性也随之单调递增
	sort.Slice(wals, func(i, j int) bool {
		indexI := walFileToMemTableIndex(wals[i].Name())
		indexJ := walFileToMemTableIndex(wals[j].Name())
		return indexI < indexJ
	})

	// 2 依次还原 memtable，添加到内存和 channel
	for i := 0; i < len(wals); i++ {
		name := wals[i].Name()
		file := path.Join(t.conf.Dir, "walfile", name)

		// 构建与 wal 文件对应的 walReader
		walReader, err := wal.NewWALReader(file)
		if err != nil {
			return err
		}
		defer walReader.Close()
		// 通过 reader 读取 wal 文件内容，将数据注入到 memtable 中
		memtable := t.conf.MemTableConstructor()
		if err = walReader.RestoreToMemtable(memtable); err != nil {
			return err
		}
		if i == len(wals)-1 { // 倘若是最后一个 wal 文件，则 memtable 作为读写 memtable
			t.memTable = memtable
			t.memTableIndex = walFileToMemTableIndex(name)
			t.walWriter, _ = wal.NewWALWriter(file)
		} else { // memtable 作为只读 memtable，需要追加到只读 slice 以及 channel 中，继续推进完成溢写落盘流程
			item := &memTableCompactItem{
				walFile:  file,
				memTable: memtable,
			}

			t.rOnlyMemTable = append(t.rOnlyMemTable, item)
			go func(compactItem *memTableCompactItem) {
				t.memCompactC <- compactItem
			}(item)
		}
	}
	return nil
}

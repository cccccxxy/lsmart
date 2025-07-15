package lsmart

import (
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/cccccxxy/lsmart/filter"
	"github.com/cccccxxy/lsmart/memtable"
)

// Config lsm tree 配置项聚合
type Config struct {
	Dir      string // sst 文件存放的目录
	// 单层分组相关配置（替代原来的多层结构）
	GroupSize        int    // 每个分组最大的 sstable 文件个数，默认 10 个
	GroupSSTSize     uint64 // 每个分组中每个 sstable 文件的大小，默认 10MB（比原来更大）
	MaxGroups        int    // 最大分组数量，默认 100 个分组
	CompactionRatio  float64 // 压缩触发比例，当分组达到该比例时触发压缩，默认 0.8

	// sst 相关
	SSTDataBlockSize int // sst table 中 block 大小 默认 16KB
	SSTFooterSize    int // sst table 中 footer 部分大小. 固定为 32B

	Filter              filter.Filter                // 过滤器. 默认使用布隆过滤器
	MemTableConstructor memtable.MemTableConstructor // memtable 构造器，默认为跳表
}

// NewConfig 配置文件构造器.
func NewConfig(dir string, opts ...ConfigOption) (*Config, error) {
	c := Config{
		Dir:           dir, // sstable 文件所在的目录路径
		SSTFooterSize: 32,  // 对应 4 个 uint64，共 32 byte
	}

	// 加载配置项
	for _, opt := range opts {
		opt(&c)
	}

	// 兜底修复
	repaire(&c)

	return &c, c.check() // 校验一下配置是否合法，主要是 check 存放 sst 文件和 wal 文件的目录，如果有缺失则进行目录创建
}

// 校验一下配置是否合法，主要是 check 存放 sst 文件和 wal 文件的目录，如果有缺失则进行目录创建
func (c *Config) check() error {
	// sstable 文件目录确保存在
	if _, err := os.ReadDir(c.Dir); err != nil {
		_, ok := err.(*fs.PathError)
		if !ok || !strings.HasSuffix(err.Error(), "no such file or directory") {
			return err
		}
		if err = os.Mkdir(c.Dir, os.ModePerm); err != nil {
			return err
		}
	}

	// wal 文件目录确保存在
	walDir := path.Join(c.Dir, "walfile")
	if _, err := os.ReadDir(walDir); err != nil {
		_, ok := err.(*fs.PathError)
		if !ok || !strings.HasSuffix(err.Error(), "no such file or directory") {
			return err
		}
		if err = os.Mkdir(walDir, os.ModePerm); err != nil {
			return err
		}
	}

	return nil
}

// ConfigOption 配置项
type ConfigOption func(*Config)

// WithGroupSize 每个分组最大的 sstable 文件个数. 默认为 10 个.
func WithGroupSize(groupSize int) ConfigOption {
	return func(c *Config) {
		c.GroupSize = groupSize
	}
}

// WithGroupSSTSize 每个分组中每个 sstable 文件的大小，单位 byte. 默认为 10 MB.
func WithGroupSSTSize(groupSSTSize uint64) ConfigOption {
	return func(c *Config) {
		c.GroupSSTSize = groupSSTSize
	}
}

// WithMaxGroups 最大分组数量. 默认为 100 个分组.
func WithMaxGroups(maxGroups int) ConfigOption {
	return func(c *Config) {
		c.MaxGroups = maxGroups
	}
}

// WithCompactionRatio 压缩触发比例. 默认为 0.8.
func WithCompactionRatio(ratio float64) ConfigOption {
	return func(c *Config) {
		c.CompactionRatio = ratio
	}
}

// WithSSTDataBlockSize sstable 中每个 block 块的大小限制. 默认为 16KB.
func WithSSTDataBlockSize(sstDataBlockSize int) ConfigOption {
	return func(c *Config) {
		c.SSTDataBlockSize = sstDataBlockSize
	}
}

// WithFilter 注入过滤器的具体实现. 默认使用本项目下实现的布隆过滤器 bloom filter.
func WithFilter(filter filter.Filter) ConfigOption {
	return func(c *Config) {
		c.Filter = filter
	}
}

// WithMemtableConstructor 注入有序表构造器. 默认使用本项目下实现的跳表 skiplist.
func WithMemtableConstructor(memtableConstructor memtable.MemTableConstructor) ConfigOption {
	return func(c *Config) {
		c.MemTableConstructor = memtableConstructor
	}
}

func repaire(c *Config) {
	// 每个分组默认为 10 个 sstable 文件.
	if c.GroupSize <= 0 {
		c.GroupSize = 10
	}

	// 每个分组中每个 sstable 文件默认大小限制为 10MB.
	if c.GroupSSTSize <= 0 {
		c.GroupSSTSize = 10 * 1024 * 1024 // 10MB
	}

	// 默认最大 100 个分组.
	if c.MaxGroups <= 0 {
		c.MaxGroups = 100
	}

	// 默认压缩触发比例为 0.8.
	if c.CompactionRatio <= 0 || c.CompactionRatio >= 1 {
		c.CompactionRatio = 0.8
	}

	// sstable 中每个 block 块的大小限制. 默认为 16KB.
	if c.SSTDataBlockSize <= 0 {
		c.SSTDataBlockSize = 16 * 1024 // 16KB
	}
	// 注入过滤器的具体实现. 默认使用本项目下实现的布隆过滤器 bloom filter.
	if c.Filter == nil {
		c.Filter, _ = filter.NewBloomFilter(1024)
	}

	// 注入有序表构造器. 默认使用本项目下实现的跳表 skiplist.
	if c.MemTableConstructor == nil {
		c.MemTableConstructor = memtable.NewSkiplist
	}
}
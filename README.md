<p align="center">
<img src="https://github.com/xiaoxuxiansheng/golsm/blob/main/img/golsm_page.png" />
<b>lsmart: 基于 go 语言实现的单层分组 LSM-tree</b>
<br/><br/>
</p>

## 📚 前言
本项目基于传统多层LSM-tree进行了重新设计，采用单层分组架构，简化了压缩逻辑，提高了大文件处理效率。

## 📖 简介
100% 纯度 go 语言实现的单层分组 LSM-tree 框架，专门优化写密集型 KV 存储场景。

## 🏗️ 架构特点

### 单层分组设计
- **传统多层LSM-tree**: Level 0 → Level 1 → Level 2 → ... → Level N
- **新单层分组设计**: MemTable → Group 1, Group 2, ..., Group N

### 分组特性
- 每个分组包含多个SST文件（默认最多10个）
- SST文件更大（默认10MB vs 传统1MB）
- 分组内SST文件按时间倒序查找
- 分组间按创建时间排序

### 压缩策略
- **分组内压缩**: 当分组内SST文件数量达到阈值时，合并为更少的大文件
- **无跨层压缩**: 简化了传统LSM-tree的多层压缩逻辑
- **更高效**: 减少了压缩频率，提高了写入性能

## 💡 技术优势

1. **简化架构**: 单层设计减少了复杂的多层管理逻辑
2. **更大文件**: 10MB SST文件减少了文件数量，提高IO效率
3. **灵活分组**: 每个分组独立管理，便于并行处理
4. **高写入性能**: 减少压缩频率，优化写入路径
5. **向后兼容**: 支持从传统多层LSM-tree迁移

## 🖥 使用示例
```go
func Test_LSM_UseCase(t *testing.T) {
	// 1 构造配置文件
	conf, _ := NewConfig("./lsm", // lsm sstable 文件的存放目录
		WithGroupSize(10),              // 每个分组最多10个SST文件
		WithGroupSSTSize(10*1024*1024), // 每个SST文件10MB
		WithMaxGroups(100),             // 最多100个分组
		WithCompactionRatio(0.8),       // 80%时触发分组压缩
		WithSSTDataBlockSize(16*1024),  // 每个block 16KB
	)

	// 2 创建一个 lsm tree 实例
	lsmTree, _ := NewTree(conf)
	defer lsmTree.Close()

	// 3 写入数据
	_ = lsmTree.Put([]byte{1}, []byte{2})

	// 4 读取数据
	v, _, _ := lsmTree.Get([]byte{1})

	t.Log(v)
}
```

## 📊 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| GroupSize | 10 | 每个分组最大SST文件数量 |
| GroupSSTSize | 10MB | 每个SST文件大小 |
| MaxGroups | 100 | 最大分组数量 |
| CompactionRatio | 0.8 | 压缩触发比例 |
| SSTDataBlockSize | 16KB | 数据块大小 |

## 🔄 文件格式

### SST文件命名
- **新格式**: `g{groupID}_{seq}.sst` (如: g1_001.sst)
- **兼容格式**: `{level}_{seq}.sst` (自动转换为分组格式)

### 数据读取优先级
1. Active MemTable (最新数据)
2. ReadOnly MemTable (按时间倒序)
3. 分组SST文件 (按分组倒序，分组内按文件倒序)

## 🚀 性能特点

- **写入优化**: 更大的SST文件减少压缩频率
- **读取优化**: 布隆过滤器 + 索引快速定位
- **空间优化**: 前缀压缩减少存储空间
- **并发优化**: 分组级别的细粒度锁

## 📈 适用场景

- 日志系统
- 时序数据库
- 写密集型应用
- 大数据量存储
- 需要高写入吞吐的系统
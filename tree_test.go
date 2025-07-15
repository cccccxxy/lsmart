package lsmart
import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)
func TestSingleLayerGroupLSM(t *testing.T) {
	// 清理测试目录
	testDir := "./test_lsm"
	os.RemoveAll(testDir)
	// 注释掉自动清理，保留数据目录
	// defer os.RemoveAll(testDir)

	// 1 构造配置文件
	conf, err := NewConfig(testDir,
		WithGroupSize(3),               // 每个分组最多3个SST文件（测试用）
		WithGroupSSTSize(1024),         // 每个SST文件1KB（测试用）
		WithMaxGroups(10),              // 最多10个分组
		WithCompactionRatio(0.7),       // 70%时触发分组压缩
		WithSSTDataBlockSize(256),      // 每个block 256B（测试用）
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}
	// 2 创建一个 lsm tree 实例
	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()

	// 3 写入测试数据
	testData := map[string]string{
		"key1":  "value1",
		"key2":  "value2",
		"key3":  "value3",
		"key4":  "value4",
		"key5":  "value5",
		"key10": "value10",
		"key20": "value20",
		"key30": "value30",
	}

	t.Log("开始写入数据...")
	for key, value := range testData {
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入数据失败 %s: %v", key, err)
		}
	}

	// 4 验证数据读取
	t.Log("开始验证数据读取...")
	for key, expectedValue := range testData {
		value, exists, err := lsmTree.Get([]byte(key))
		if err != nil {
			t.Fatalf("读取数据失败 %s: %v", key, err)
		}
		if !exists {
			t.Fatalf("数据不存在: %s", key)
		}
		if string(value) != expectedValue {
			t.Fatalf("数据不匹配 %s: 期望 %s, 实际 %s", key, expectedValue, string(value))
		}
	}

	// 5 测试数据覆盖
	t.Log("测试数据覆盖...")
	err = lsmTree.Put([]byte("key1"), []byte("new_value1"))
	if err != nil {
		t.Fatalf("覆盖数据失败: %v", err)
	}
	value, exists, err := lsmTree.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("读取覆盖数据失败: %v", err)
	}
	if !exists {
		t.Fatal("覆盖后数据不存在")
	}
	if string(value) != "new_value1" {
		t.Fatalf("覆盖数据不匹配: 期望 new_value1, 实际 %s", string(value))
	}
	// 6 测试不存在的key
	t.Log("测试不存在的key...")
	_, exists, err = lsmTree.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("查询不存在key失败: %v", err)
	}
	if exists {
		t.Fatal("不存在的key返回了存在")
	}

	t.Logf("所有测试通过! 数据目录保存在: %s", testDir)
}

// TestShowDataStructure 显示数据目录结构
func TestShowDataStructure(t *testing.T) {
	testDir := "./test_structure"
	os.RemoveAll(testDir)

	conf, _ := NewConfig(testDir,
		WithGroupSize(2),
		WithGroupSSTSize(500),
		WithMaxGroups(5),
		WithCompactionRatio(0.6),
		WithSSTDataBlockSize(128),
	)
	lsmTree, _ := NewTree(conf)
	defer lsmTree.Close()
	// 写入一些数据
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d", i)
		lsmTree.Put([]byte(key), []byte(value))
	}

	t.Logf("数据结构目录: %s", testDir)
	t.Log("目录结构将在测试完成后保留，可以查看生成的SST文件和WAL文件")
}
// TestCreateSSTFiles 测试SST文件生成
func TestCreateSSTFiles(t *testing.T) {
	testDir := "./test_sst_generation"
	os.RemoveAll(testDir)
	// 设置更小的阈值来更容易触发SST文件生成
	conf, err := NewConfig(testDir,
		WithGroupSize(2),               // 每个分组最多2个SST文件
		WithGroupSSTSize(200),          // 每个SST文件200字节（很小，容易触发）
		WithMaxGroups(5),               // 最多5个分组
		WithCompactionRatio(0.6),       // 60%时触发压缩
		WithSSTDataBlockSize(64),       // 每个block 64字节
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}
	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()
	// 写入足够的数据来触发多个SST文件生成
	t.Log("写入大量数据以触发SST文件生成...")
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("test_key_%04d", i)
		value := fmt.Sprintf("test_value_%04d_with_some_extra_data_to_make_it_larger", i)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
		// 每写入10个key就检查一下目录结构
		if i > 0 && i%10 == 0 {
			t.Logf("已写入 %d 个键值对...", i+1)
		}
	}

	// 强制等待一下，让压缩协程有时间处理
	t.Log("等待压缩处理完成...")
	time.Sleep(1 * time.Second) // 这里可以加一个短暂的sleep来等待后台协程处理
	// 验证数据完整性
	t.Log("验证数据完整性...")
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("test_key_%04d", i)
		expectedValue := fmt.Sprintf("test_value_%04d_with_some_extra_data_to_make_it_larger", i)
		value, exists, err := lsmTree.Get([]byte(key))
		if err != nil {
			t.Fatalf("读取数据失败: %v", err)
		}
		if !exists {
			t.Fatalf("数据不存在: %s", key)
		}
		if string(value) != expectedValue {
			t.Fatalf("数据不匹配: %s", key)
		}
	}

	t.Logf("测试完成! 数据目录: %s", testDir)
	t.Log("现在应该可以看到生成的SST文件了")
}

// TestSimpleSSTFileRW 测试简单的SST文件读写功能
func TestSimpleSSTFileRW(t *testing.T) {
	testDir := "./test_simple_sst"
	os.RemoveAll(testDir)

	conf, err := NewConfig(testDir,
		WithGroupSize(1),
		WithGroupSSTSize(100),
		WithMaxGroups(1),
		WithCompactionRatio(0.5),
		WithSSTDataBlockSize(32),
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}

	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()
	// 写入数据
	t.Log("写入测试数据...")
	key := "simple_key"
	value := "simple_value"
	err = lsmTree.Put([]byte(key), []byte(value))
	if err != nil {
		t.Fatalf("写入数据失败 %s: %v", key, err)
	}

	// 等待一段时间让压缩完成
	t.Log("等待压缩完成...")
	time.Sleep(100 * time.Millisecond)

	// 验证数据
	t.Log("验证数据完整性...")
	retrievedValue, exists, err := lsmTree.Get([]byte(key))
	if err != nil {
		t.Errorf("读取数据失败 %s: %v", key, err)
	}
	if !exists {
		t.Errorf("数据不存在: %s", key)
	}
	if string(retrievedValue) != value {
		t.Errorf("数据不匹配 %s: 期望 %s, 实际 %s", key, value, string(retrievedValue))
	}
	t.Logf("数据目录: %s", testDir)

	// 显示生成的文件
	if files, err := os.ReadDir(testDir); err == nil {
		t.Log("生成的文件:")
		for _, file := range files {
			if !file.IsDir() {
				t.Logf("  %s", file.Name())
			}
		}
		// 显示WAL文件
		if walFiles, err := os.ReadDir(testDir + "/walfile"); err == nil {
			t.Log("WAL文件:")
			for _, file := range walFiles {
				t.Logf("  walfile/%s", file.Name())
			}
		}
	}
}

// TestSSTFilesWithDelay 测试SST文件生成和数据读取（加延迟等待压缩完成）
func TestSSTFilesWithDelay(t *testing.T) {
	testDir := "./test_sst_with_delay"
	os.RemoveAll(testDir)
	conf, err := NewConfig(testDir,
		WithGroupSize(3),
		WithGroupSSTSize(300),          // 300字节，适中的大小
		WithMaxGroups(10),
		WithCompactionRatio(0.8),
		WithSSTDataBlockSize(128),
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}
	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()
	// 写入数据
	testData := make(map[string]string)
	t.Log("写入测试数据...")
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_some_extra_data_to_reach_threshold", i)
		testData[key] = value
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入数据失败 %s: %v", key, err)
		}
	}
	// 等待一段时间让压缩完成
	t.Log("等待压缩完成...")
	time.Sleep(100 * time.Millisecond)

	// 验证数据
	t.Log("验证数据完整性...")
	successCount := 0
	for key, expectedValue := range testData {
		value, exists, err := lsmTree.Get([]byte(key))
		if err != nil {
			t.Errorf("读取数据失败 %s: %v", key, err)
			continue
		}
		if !exists {
			t.Errorf("数据不存在: %s", key)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("数据不匹配 %s: 期望 %s, 实际 %s", key, expectedValue, string(value))
			continue
		}
		successCount++
	}

	t.Logf("成功验证 %d/%d 个数据项", successCount, len(testData))
	t.Logf("数据目录: %s", testDir)
	// 显示生成的文件
	if files, err := os.ReadDir(testDir); err == nil {
		t.Log("生成的文件:")
		for _, file := range files {
			if !file.IsDir() {
				t.Logf("  %s", file.Name())
			}
		}
		// 显示WAL文件
		if walFiles, err := os.ReadDir(testDir + "/walfile"); err == nil {
			t.Log("WAL文件:")
			for _, file := range walFiles {
				t.Logf("  walfile/%s", file.Name())
			}
		}
	}
}
// TestDebugMissingKeys 调试缺失的key问题
func TestDebugMissingKeys(t *testing.T) {
	testDir := "./test_debug"
	os.RemoveAll(testDir)
	
	conf, err := NewConfig(testDir,
		WithGroupSize(2),
		WithGroupSSTSize(400),
		WithMaxGroups(5),
		WithCompactionRatio(0.9), // 提高阈值，减少压缩频率
		WithSSTDataBlockSize(128),
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}

	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()
	// 先只写入前5个key进行测试
	testKeys := []string{"key_000", "key_001", "key_002", "key_003", "key_004"}
	
	t.Log("=== 写入阶段 ===")
	for i, key := range testKeys {
		value := fmt.Sprintf("value_%03d_debug_test", i)
		t.Logf("写入: %s -> %s", key, value)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入失败 %s: %v", key, err)
		}
	}

	t.Log("=== 立即读取测试（从MemTable） ===")
	for i, key := range testKeys {
		expectedValue := fmt.Sprintf("value_%03d_debug_test", i)
		value, exists, err := lsmTree.Get([]byte(key))
		if err != nil {
			t.Errorf("读取错误 %s: %v", key, err)
		} else if !exists {
			t.Errorf("数据不存在（MemTable）: %s", key)
		} else if string(value) != expectedValue {
			t.Errorf("数据不匹配（MemTable）%s: 期望 %s, 实际 %s", key, expectedValue, string(value))
		} else {
			t.Logf("✓ 成功读取（MemTable）: %s", key)
		}
	}

	// 写入更多数据触发刷盘
	t.Log("=== 写入更多数据触发刷盘 ===")
	for i := 5; i < 15; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_trigger_flush_data_with_extra_content", i)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入失败 %s: %v", key, err)
		}
	}
	// 等待压缩
	t.Log("=== 等待压缩完成 ===")
	time.Sleep(200 * time.Millisecond)

	// 再次测试前5个key
	t.Log("=== 刷盘后读取测试 ===")
	for i, key := range testKeys {
		expectedValue := fmt.Sprintf("value_%03d_debug_test", i)
		value, exists, err := lsmTree.Get([]byte(key))
		if err != nil {
			t.Errorf("读取错误 %s: %v", key, err)
		} else if !exists {
			t.Errorf("❌ 数据不存在（刷盘后）: %s", key)
		} else if string(value) != expectedValue {
			t.Errorf("数据不匹配（刷盘后）%s: 期望 %s, 实际 %s", key, expectedValue, string(value))
		} else {
			t.Logf("✓ 成功读取（刷盘后）: %s", key)
		}
	}

	// 显示文件信息
	t.Log("=== 生成的文件 ===")
	if files, err := os.ReadDir(testDir); err == nil {
		for _, file := range files {
			if !file.IsDir() {
				info, _ := file.Info()
				t.Logf("文件: %s (大小: %d 字节)", file.Name(), info.Size())
			}
		}
	}

	// 显示WAL文件
	if walFiles, err := os.ReadDir(testDir + "/walfile"); err == nil {
		t.Log("WAL文件:")
		for _, file := range walFiles {
			info, _ := file.Info()
			t.Logf("  %s (大小: %d 字节)", file.Name(), info.Size())
		}
	}
}

// TestInspectMemTable 检查MemTable中的数据
func TestInspectMemTable(t *testing.T) {
	testDir := "./test_memtable_inspect"
	os.RemoveAll(testDir)
	
	conf, err := NewConfig(testDir,
		WithGroupSize(3),
		WithGroupSSTSize(300),          // 300字节，适中的大小
		WithMaxGroups(10),
		WithCompactionRatio(0.8),
		WithSSTDataBlockSize(128),
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}
	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()
	// 写入测试数据（与之前失败的测试相同的数据）
	testData := make(map[string]string)
	t.Log("=== 写入测试数据 ===")
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_some_extra_data_to_reach_threshold", i)
		testData[key] = value
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入数据失败 %s: %v", key, err)
		}
	}
	// 检查活跃MemTable中的数据
	t.Log("=== 检查活跃MemTable内容 ===")
	memTableData := lsmTree.memTable.All()
	t.Logf("活跃MemTable中有 %d 个条目:", len(memTableData))
	for i, kv := range memTableData {
		t.Logf("  [%d] %s -> %s", i, string(kv.Key), string(kv.Value))
	}
	// 检查只读MemTable中的数据
	t.Log("=== 检查只读MemTable内容 ===")
	t.Logf("只读MemTable数量: %d", len(lsmTree.rOnlyMemTable))
	for i, roMemTable := range lsmTree.rOnlyMemTable {
		roData := roMemTable.memTable.All()
		t.Logf("只读MemTable[%d] 有 %d 个条目:", i, len(roData))
		for j, kv := range roData {
			t.Logf("  [%d] %s -> %s", j, string(kv.Key), string(kv.Value))
		}
	}

	// 等待压缩完成
	t.Log("=== 等待压缩完成 ===")
	time.Sleep(200 * time.Millisecond)

	// 再次检查MemTable状态
	t.Log("=== 压缩后MemTable状态 ===")
	memTableDataAfter := lsmTree.memTable.All()
	t.Logf("压缩后活跃MemTable中有 %d 个条目:", len(memTableDataAfter))
	for i, kv := range memTableDataAfter {
		t.Logf("  [%d] %s -> %s", i, string(kv.Key), string(kv.Value))
	}

	t.Logf("压缩后只读MemTable数量: %d", len(lsmTree.rOnlyMemTable))
	for i, roMemTable := range lsmTree.rOnlyMemTable {
		roData := roMemTable.memTable.All()
		t.Logf("压缩后只读MemTable[%d] 有 %d 个条目:", i, len(roData))
		for j, kv := range roData {
			t.Logf("  [%d] %s -> %s", j, string(kv.Key), string(kv.Value))
		}
	}

	// 检查生成的分组和SST文件
	t.Log("=== 检查分组信息 ===")
	t.Logf("总分组数: %d", len(lsmTree.groups))
	for i, group := range lsmTree.groups {
		t.Logf("分组[%d] ID:%d, 节点数:%d, 大小:%d字节", 
			i, group.ID(), group.NodeCount(), group.Size())
		if group.StartKey() != nil && group.EndKey() != nil {
			t.Logf("  Key范围: %s ~ %s", string(group.StartKey()), string(group.EndKey()))
		}
	}

	// 测试特定的问题key
	t.Log("=== 测试问题Key ===")
	problemKeys := []string{"key_000", "key_001", "key_002"}
	for _, key := range problemKeys {
		value, exists, err := lsmTree.Get([]byte(key))
		if err != nil {
			t.Logf("❌ %s: 读取错误 - %v", key, err)
		} else if !exists {
			t.Logf("❌ %s: 数据不存在", key)
		} else {
			t.Logf("✅ %s: 成功读取 - %s", key, string(value))
		}
	}
}
// TestDetailedMemTableInspection 详细检查MemTable内容
func TestDetailedMemTableInspection(t *testing.T) {
	testDir := "./test_detailed_inspect"
	os.RemoveAll(testDir)
	
	conf, err := NewConfig(testDir,
		WithGroupSize(3),
		WithGroupSSTSize(300),
		WithMaxGroups(10),
		WithCompactionRatio(0.8),
		WithSSTDataBlockSize(128),
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}

	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()
	// 写入测试数据
	t.Log("=== 写入测试数据 ===")
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_some_extra_data_to_reach_threshold", i)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入数据失败 %s: %v", key, err)
		}
	}
	// 等待压缩
	time.Sleep(200 * time.Millisecond)

	// 详细检查每个只读MemTable的所有内容
	t.Log("=== 详细检查只读MemTable ===")
	allKeysInMemTables := make(map[string]bool)
	
	for i, roMemTable := range lsmTree.rOnlyMemTable {
		roData := roMemTable.memTable.All()
		t.Logf("只读MemTable[%d] 详细内容 (%d个条目):", i, len(roData))
		for j, kv := range roData {
			keyStr := string(kv.Key)
			allKeysInMemTables[keyStr] = true
			t.Logf("  [%d] %s -> %s", j, keyStr, string(kv.Value))
		}
		t.Log("  ---")
	}

	// 检查所有预期的key是否存在
	t.Log("=== 检查Key分布 ===")
	problemKeys := []string{"key_000", "key_001", "key_002"}
	for _, key := range problemKeys {
		if allKeysInMemTables[key] {
			t.Logf("✅ %s: 在只读MemTable中找到", key)
		} else {
			t.Logf("❌ %s: 在只读MemTable中未找到", key)
		}
	}

	// 检查哪些key在只读MemTable中
	t.Log("=== 只读MemTable中的所有Key ===")
	for key := range allKeysInMemTables {
		t.Logf("  %s", key)
	}

	// 手动测试Get方法对只读MemTable的访问
	t.Log("=== 手动测试只读MemTable读取 ===")
	for _, key := range problemKeys {
		t.Logf("测试读取: %s", key)
		// 直接访问只读MemTable
		found := false
		for i := len(lsmTree.rOnlyMemTable) - 1; i >= 0; i-- {
			value, ok := lsmTree.rOnlyMemTable[i].memTable.Get([]byte(key))
			if ok {
				t.Logf("  ✅ 在只读MemTable[%d]中找到: %s", i, string(value))
				found = true
				break
			}
		}
		if !found {
			t.Logf("  ❌ 在所有只读MemTable中都未找到")
		}
		
		// 通过Get方法读取
		value, exists, err := lsmTree.Get([]byte(key))
		if err != nil {
			t.Logf("  Get方法错误: %v", err)
		} else if exists {
			t.Logf("  Get方法成功: %s", string(value))
		} else {
			t.Logf("  Get方法失败: 数据不存在")
		}
	}
}
// TestDataFlow 跟踪数据从MemTable到SST文件的流向
func TestDataFlow(t *testing.T) {
	testDir := "./test_data_flow"
	os.RemoveAll(testDir)
	
	conf, err := NewConfig(testDir,
		WithGroupSize(3),
		WithGroupSSTSize(300),
		WithMaxGroups(10),
		WithCompactionRatio(0.8),
		WithSSTDataBlockSize(128),
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}

	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()
	// 分批写入，观察每次的状态变化
	t.Log("=== 第一批数据 (key_000 - key_004) ===")
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_batch1", i)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}
	
	t.Logf("活跃MemTable大小: %d", len(lsmTree.memTable.All()))
	t.Logf("只读MemTable数量: %d", len(lsmTree.rOnlyMemTable))

	t.Log("=== 第二批数据 (key_005 - key_009) ===")
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_batch2_long_data_to_trigger_flush", i)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}
	
	t.Logf("活跃MemTable大小: %d", len(lsmTree.memTable.All()))
	t.Logf("只读MemTable数量: %d", len(lsmTree.rOnlyMemTable))
	t.Log("=== 第三批数据 (key_010 - key_019) ===")
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_batch3_even_longer_data_to_definitely_trigger_flush", i)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入失败: %v", err)
		}
	}
	
	t.Logf("活跃MemTable大小: %d", len(lsmTree.memTable.All()))
	t.Logf("只读MemTable数量: %d", len(lsmTree.rOnlyMemTable))
	// 等待压缩完成
	t.Log("=== 等待压缩完成 ===")
	time.Sleep(300 * time.Millisecond)

	// 检查最终状态
	t.Log("=== 最终状态 ===")
	t.Logf("活跃MemTable大小: %d", len(lsmTree.memTable.All()))
	t.Logf("只读MemTable数量: %d", len(lsmTree.rOnlyMemTable))
	t.Logf("分组数量: %d", len(lsmTree.groups))

	// 检查SST文件
	if files, err := os.ReadDir(testDir); err == nil {
		t.Log("生成的SST文件:")
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".sst") {
				info, _ := file.Info()
				t.Logf("  %s (大小: %d字节)", file.Name(), info.Size())
			}
		}
	}
	// 检查每个批次的数据能否正确读取
	t.Log("=== 验证各批次数据 ===")
	batches := []struct{
		name string
		start, end int
		valuePattern string
	}{
		{"第一批", 0, 5, "value_%03d_batch1"},
		{"第二批", 5, 10, "value_%03d_batch2_long_data_to_trigger_flush"},
		{"第三批", 10, 20, "value_%03d_batch3_even_longer_data_to_definitely_trigger_flush"},
	}

	for _, batch := range batches {
		t.Logf("检查%s数据:", batch.name)
		successCount := 0
		for i := batch.start; i < batch.end; i++ {
			key := fmt.Sprintf("key_%03d", i)
			expectedValue := fmt.Sprintf(batch.valuePattern, i)
			value, exists, err := lsmTree.Get([]byte(key))
			if err != nil {
				t.Logf("  ❌ %s: 读取错误 - %v", key, err)
			} else if !exists {
				t.Logf("  ❌ %s: 数据不存在", key)
			} else if string(value) != expectedValue {
				t.Logf("  ❌ %s: 值不匹配 - 期望%s，实际%s", key, expectedValue, string(value))
			} else {
				t.Logf("  ✅ %s: 成功", key)
				successCount++
			}
		}
		t.Logf("  %s成功率: %d/%d", batch.name, successCount, batch.end-batch.start)
	}
}
// TestInspectGroupsAndNodes 详细检查每个分组和节点的内容
func TestInspectGroupsAndNodes(t *testing.T) {
	testDir := "./test_inspect_groups"
	os.RemoveAll(testDir)
	
	conf, err := NewConfig(testDir,
		WithGroupSize(3),
		WithGroupSSTSize(300),
		WithMaxGroups(10),
		WithCompactionRatio(0.8),
		WithSSTDataBlockSize(128),
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}

	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()

	// 写入测试数据
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_some_extra_data_to_reach_threshold", i)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
	}

	// 等待压缩
	time.Sleep(200 * time.Millisecond)
	// 详细检查每个分组和节点
	t.Log("=== 详细检查分组和节点 ===")
	problemKeys := []string{"key_000", "key_001", "key_002"}
	allFoundKeys := make(map[string]string) // key -> location

	for i, group := range lsmTree.groups {
		t.Logf("分组[%d] ID:%d, 节点数:%d", i, group.ID(), group.NodeCount())
		if group.StartKey() != nil && group.EndKey() != nil {
			t.Logf("  Key范围: %s ~ %s", string(group.StartKey()), string(group.EndKey()))
		}

		// 检查分组中的每个节点
		nodes := group.GetNodes()
		for j, node := range nodes {
			t.Logf("  节点[%d] 文件:%s, 大小:%d", j, node.file, node.Size())
			t.Logf("    Key范围: %s ~ %s", string(node.Start()), string(node.End()))
			// 直接从节点读取所有数据
			kvs, err := node.GetAll()
			if err != nil {
				t.Logf("    ❌ 读取节点数据失败: %v", err)
				continue
			}

			t.Logf("    包含 %d 个key:", len(kvs))
			for _, kv := range kvs {
				keyStr := string(kv.Key)
				location := fmt.Sprintf("分组%d-节点%d", group.ID(), j)
				allFoundKeys[keyStr] = location
				
				// 检查是否是问题key
				for _, problemKey := range problemKeys {
					if keyStr == problemKey {
						t.Logf("    ✅ 找到问题key: %s (在%s)", problemKey, location)
					}
				}
			}
		}
	}

	// 检查问题key的位置
	t.Log("=== 问题Key位置分析 ===")
	for _, problemKey := range problemKeys {
		if location, found := allFoundKeys[problemKey]; found {
			t.Logf("✅ %s: 在%s中找到", problemKey, location)
		} else {
			t.Logf("❌ %s: 在所有分组和节点中都未找到", problemKey)
		}
	}
	// 测试直接从分组读取问题key
	t.Log("=== 测试分组读取 ===")
	for _, problemKey := range problemKeys {
		for i, group := range lsmTree.groups {
			value, ok, err := group.Get([]byte(problemKey))
			if err != nil {
				t.Logf("分组[%d]读取%s错误: %v", i, problemKey, err)
			} else if ok {
				t.Logf("✅ 分组[%d]成功读取%s: %s", i, problemKey, string(value))
			} else {
				t.Logf("❌ 分组[%d]未找到%s", i, problemKey)
			}
		}
	}

	// 最终测试Get方法
	t.Log("=== 最终Get方法测试 ===")
	for _, problemKey := range problemKeys {
		value, exists, err := lsmTree.Get([]byte(problemKey))
		if err != nil {
			t.Logf("❌ %s: Get错误 - %v", problemKey, err)
		} else if !exists {
			t.Logf("❌ %s: Get返回不存在", problemKey)
		} else {
			t.Logf("✅ %s: Get成功 - %s", problemKey, string(value))
		}
	}
}
// TestInspectNodeKeys 详细检查节点的Key范围
func TestInspectNodeKeys(t *testing.T) {
	testDir := "./test_node_keys"
	os.RemoveAll(testDir)
	
	conf, err := NewConfig(testDir,
		WithGroupSize(3),
		WithGroupSSTSize(300),
		WithMaxGroups(10),
		WithCompactionRatio(0.8),
		WithSSTDataBlockSize(128),
	)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}

	lsmTree, err := NewTree(conf)
	if err != nil {
		t.Fatalf("创建LSM tree失败: %v", err)
	}
	defer lsmTree.Close()

	// 写入测试数据
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d_some_extra_data_to_reach_threshold", i)
		err := lsmTree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("写入数据失败: %v", err)
		}
	}

	// 等待压缩
	time.Sleep(200 * time.Millisecond)

	// 检查第一个分组第一个节点（包含问题key的节点）
	if len(lsmTree.groups) == 0 {
		t.Fatal("没有分组")
	}

	firstGroup := lsmTree.groups[0]
	nodes := firstGroup.GetNodes()
	if len(nodes) == 0 {
		t.Fatal("第一个分组没有节点")
	}

	problemNode := nodes[0]
	t.Log("=== 问题节点详细信息 ===")
	t.Logf("节点文件: %s", problemNode.file)
	t.Logf("节点大小: %d", problemNode.Size())

	// 检查startKey和endKey的字节内容
	startKey := problemNode.Start()
	endKey := problemNode.End()
	
	t.Logf("StartKey字符串: '%s'", string(startKey))
	t.Logf("StartKey字节: %v", startKey)
	t.Logf("StartKey十六进制: %x", startKey)
	
	t.Logf("EndKey字符串: '%s'", string(endKey))
	t.Logf("EndKey字节: %v", endKey)
	t.Logf("EndKey十六进制: %x", endKey)
	// 测试问题key与startKey/endKey的比较
	problemKeys := []string{"key_000", "key_001", "key_002"}
	for _, keyStr := range problemKeys {
		key := []byte(keyStr)
		t.Logf("=== 测试 %s ===", keyStr)
		t.Logf("Key字节: %v", key)
		t.Logf("Key十六进制: %x", key)
		startCmp := bytes.Compare(key, startKey)
		endCmp := bytes.Compare(key, endKey)
		t.Logf("与StartKey比较: %d (key %s startKey)", startCmp, cmpStr(startCmp))
		t.Logf("与EndKey比较: %d (key %s endKey)", endCmp, cmpStr(endCmp))
		
		// 范围检查
		inRange := startCmp >= 0 && endCmp <= 0
		t.Logf("在范围内: %t", inRange)
		
		// 尝试直接调用节点的Get方法
		value, exists, err := problemNode.Get(key)
		if err != nil {
			t.Logf("节点Get错误: %v", err)
		} else if exists {
			t.Logf("✅ 节点Get成功: %s", string(value))
		} else {
			t.Logf("❌ 节点Get失败: 不存在")
		}
	}

	// 检查节点中实际包含的所有key
	t.Log("=== 节点实际包含的Key ===")
	kvs, err := problemNode.GetAll()
	if err != nil {
		t.Fatalf("GetAll失败: %v", err)
	}
	
	for i, kv := range kvs {
		t.Logf("[%d] Key:'%s' 字节:%v 十六进制:%x", i, string(kv.Key), kv.Key, kv.Key)
	}
}

func cmpStr(cmp int) string {
	if cmp < 0 {
		return "<"
	} else if cmp > 0 {
		return ">"
	}
	return "=="
}
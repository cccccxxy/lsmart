package lsmart

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// 简单测试辅助函数
func setupTest(t *testing.T) (*Tree, func()) {
	testDir := fmt.Sprintf("./test_%d", time.Now().UnixNano())

	config, err := NewConfig(testDir)
	if err != nil {
		t.Fatalf("创建配置失败: %v", err)
	}

	tree, err := NewTree(config)
	if err != nil {
		t.Fatalf("创建LSM树失败: %v", err)
	}

	cleanup := func() {
		tree.Close()
		os.RemoveAll(testDir)
	}

	return tree, cleanup
}

// TestSimpleWriteRead 测试简单的写入和读取
func TestSimpleWriteRead(t *testing.T) {
	tree, cleanup := setupTest(t)
	defer cleanup()

	// 测试写入
	key := "test_key"
	value := "test_value"

	err := tree.Put([]byte(key), []byte(value))
	if err != nil {
		t.Fatalf("写入失败: %v", err)
	}

	// 测试读取
	result, found, err := tree.Get([]byte(key))
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if !found {
		t.Fatal("未找到数据")
	}

	if string(result) != value {
		t.Errorf("值不匹配: 期望=%s, 实际=%s", value, string(result))
	}

	t.Logf("✅ 测试成功: %s = %s", key, string(result))
}

// TestMultipleKeys 测试大量键值对
func TestMultipleKeys(t *testing.T) {
	tree, cleanup := setupTest(t)
	defer cleanup()
	// 增大数据量到10000个键值对
	keyCount := 50000

	t.Logf("开始写入 %d 个键值对...", keyCount)
	writeStart := time.Now()

	// 写入大量键值对
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key_%08d", i)
		value := fmt.Sprintf("value_%08d_timestamp_%d", i, time.Now().UnixNano())

		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("写入失败 %s: %v", key, err)
		}

		if (i+1)%1000 == 0 {
			t.Logf("已写入 %d 个键值对", i+1)
		}
	}

	writeDuration := time.Since(writeStart)
	writeQPS := float64(keyCount) / writeDuration.Seconds()
	t.Logf("✅ 写入完成: %d 个键值对，耗时: %v, QPS: %.2f", keyCount, writeDuration, writeQPS)

	// 等待压缩完成
	time.Sleep(500 * time.Millisecond)
	// 读取并验证所有数据
	t.Log("开始验证所有数据...")
	readStart := time.Now()
	successCount := 0

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key_%08d", i)
		value, found, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("读取失败 %s: %v", key, err)
			continue
		}

		if !found {
			t.Errorf("未找到键: %s", key)
			continue
		}

		expectedPrefix := fmt.Sprintf("value_%08d_timestamp_", i)
		if len(string(value)) >= len(expectedPrefix) &&
			string(value)[:len(expectedPrefix)] == expectedPrefix {
			successCount++
		} else {
			t.Errorf("值格式不匹配: key=%s", key)
		}

		if (i+1)%1000 == 0 {
			t.Logf("已验证 %d 个键值对", i+1)
		}
	}

	readDuration := time.Since(readStart)
	readQPS := float64(keyCount) / readDuration.Seconds()
	t.Logf("✅ 验证完成: 成功 %d/%d，耗时: %v, QPS: %.2f", successCount, keyCount, readDuration, readQPS)

	if successCount != keyCount {
		t.Errorf("数据完整性检查失败: 期望 %d，实际 %d", keyCount, successCount)
	}
}

// TestBatchWrite 测试超大批量写入
func TestBatchWrite(t *testing.T) {
	tree, cleanup := setupTest(t)
	defer cleanup()

	// 增大批量大小到50000
	batchSize := 50000
	t.Logf("开始超大批量写入测试: %d 条记录", batchSize)
	writeStart := time.Now()
	// 批量写入
	for i := 0; i < batchSize; i++ {
		key := fmt.Sprintf("batch_%010d", i)
		value := fmt.Sprintf("large_batch_value_%010d_%d", i, time.Now().UnixNano())

		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("批量写入失败 %s: %v", key, err)
		}

		if (i+1)%5000 == 0 {
			t.Logf("已写入 %d 条记录", i+1)
		}
	}
	writeDuration := time.Since(writeStart)
	writeQPS := float64(batchSize) / writeDuration.Seconds()
	t.Logf("✅ 批量写入完成: %d 条记录，耗时: %v, QPS: %.2f", batchSize, writeDuration, writeQPS)

	// 等待压缩
	time.Sleep(2 * time.Second)

	// 验证随机抽样的数据
	t.Log("开始随机验证...")
	readStart := time.Now()
	sampleSize := 5000 // 抽样验证5000条
	successCount := 0
	for i := 0; i < sampleSize; i++ {
		// 随机选择要验证的索引
		testIndex := (i * 9973) % batchSize // 使用质数9973来生成随机分布
		key := fmt.Sprintf("batch_%010d", testIndex)

		value, found, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("随机验证读取失败 %s: %v", key, err)
		} else if !found {
			t.Errorf("随机验证数据未找到: %s", key)
		} else {
			expectedPrefix := fmt.Sprintf("large_batch_value_%010d_", testIndex)
			if len(string(value)) >= len(expectedPrefix) &&
				string(value)[:len(expectedPrefix)] == expectedPrefix {
				successCount++
			} else {
				t.Errorf("随机验证数据格式错误: %s", key)
			}
		}
		if (i+1)%500 == 0 {
			t.Logf("已随机验证 %d 条记录", i+1)
		}
	}
	readDuration := time.Since(readStart)
	readQPS := float64(sampleSize) / readDuration.Seconds()
	t.Logf("✅ 随机验证完成: 成功 %d/%d，耗时: %v, QPS: %.2f", successCount, sampleSize, readDuration, readQPS)

	if successCount < sampleSize*95/100 { // 允许5%的误差
		t.Errorf("随机验证成功率过低: %d/%d", successCount, sampleSize)
	}
}

// TestUpdateKey 测试大量更新操作
func TestUpdateKey(t *testing.T) {
	tree, cleanup := setupTest(t)
	defer cleanup()
	updateCount := 5000
	t.Logf("开始大量更新操作测试: %d 次更新", updateCount)

	// 首先写入初始数据
	for i := 0; i < updateCount; i++ {
		key := fmt.Sprintf("update_%08d", i)
		originalValue := fmt.Sprintf("original_value_%08d", i)
		err := tree.Put([]byte(key), []byte(originalValue))
		if err != nil {
			t.Errorf("写入初始值失败: %v", err)
		}
	}

	t.Log("初始数据写入完成，开始更新操作...")
	updateStart := time.Now()

	// 进行更新操作
	for i := 0; i < updateCount; i++ {
		key := fmt.Sprintf("update_%08d", i)
		updatedValue := fmt.Sprintf("updated_value_%08d_%d", i, time.Now().UnixNano())
		err := tree.Put([]byte(key), []byte(updatedValue))
		if err != nil {
			t.Errorf("更新值失败: %v", err)
		}

		if (i+1)%500 == 0 {
			t.Logf("已更新 %d 条记录", i+1)
		}
	}
	updateDuration := time.Since(updateStart)
	updateQPS := float64(updateCount) / updateDuration.Seconds()
	t.Logf("✅ 更新操作完成: %d 次更新，耗时: %v, QPS: %.2f", updateCount, updateDuration, updateQPS)
	// 等待压缩
	time.Sleep(500 * time.Millisecond)

	// 验证更新后的值
	t.Log("验证更新后的数据...")
	verifyStart := time.Now()
	successCount := 0
	for i := 0; i < updateCount; i++ {
		key := fmt.Sprintf("update_%08d", i)

		value, found, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("验证更新失败: %v", err)
		} else if !found {
			t.Errorf("更新后数据丢失: %s", key)
		} else {
			expectedPrefix := fmt.Sprintf("updated_value_%08d_", i)
			if len(string(value)) >= len(expectedPrefix) &&
				string(value)[:len(expectedPrefix)] == expectedPrefix {
				successCount++
			} else {
				t.Errorf("更新后数据格式错误: %s", key)
			}
		}

		if (i+1)%500 == 0 {
			t.Logf("已验证更新 %d 条记录", i+1)
		}
	}
	verifyDuration := time.Since(verifyStart)
	verifyQPS := float64(updateCount) / verifyDuration.Seconds()
	t.Logf("✅ 更新验证完成: 成功 %d/%d，耗时: %v, QPS: %.2f", successCount, updateCount, verifyDuration, verifyQPS)

	if successCount != updateCount {
		t.Errorf("更新验证失败: 期望 %d，实际 %d", updateCount, successCount)
	}
}

// TestNonExistent 测试不存在的键
func TestNonExistent(t *testing.T) {
	tree, cleanup := setupTest(t)
	defer cleanup()

	// 测试大量不存在的键
	nonExistentCount := 10000
	t.Logf("测试 %d 个不存在的键", nonExistentCount)

	testStart := time.Now()
	correctCount := 0

	for i := 0; i < nonExistentCount; i++ {
		key := fmt.Sprintf("nonexistent_%08d", i)
		value, found, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("查询不存在键失败: %v", err)
		} else if found {
			t.Errorf("错误地找到了不存在的键 %s: %s", key, string(value))
		} else {
			correctCount++
		}
		if (i+1)%1000 == 0 {
			t.Logf("已测试 %d 个不存在的键", i+1)
		}
	}

	testDuration := time.Since(testStart)
	testQPS := float64(nonExistentCount) / testDuration.Seconds()
	t.Logf("✅ 不存在键测试完成: 正确处理 %d/%d，耗时: %v, QPS: %.2f",
		correctCount, nonExistentCount, testDuration, testQPS)
	if correctCount != nonExistentCount {
		t.Errorf("不存在键测试失败: 期望 %d，实际 %d", nonExistentCount, correctCount)
	}
}

// TestMixedOperations 测试混合读写操作
func TestMixedOperations(t *testing.T) {
	tree, cleanup := setupTest(t)
	defer cleanup()

	operationCount := 20000
	t.Logf("开始混合操作测试: %d 次操作", operationCount)
	// 先写入一些基础数据
	baseDataCount := 5000
	for i := 0; i < baseDataCount; i++ {
		key := fmt.Sprintf("base_%08d", i)
		value := fmt.Sprintf("base_value_%08d", i)
		tree.Put([]byte(key), []byte(value))
	}
	t.Log("基础数据写入完成，开始混合操作...")
	mixedStart := time.Now()
	writeCount := 0
	readCount := 0
	updateCount := 0

	for i := 0; i < operationCount; i++ {
		operation := i % 3

		switch operation {
		case 0: // 写入新数据
			key := fmt.Sprintf("mixed_write_%08d", i)
			value := fmt.Sprintf("mixed_value_%08d_%d", i, time.Now().UnixNano())
			err := tree.Put([]byte(key), []byte(value))
			if err != nil {
				t.Errorf("混合写入失败: %v", err)
			}
			writeCount++
		case 1: // 读取已有数据
			readIndex := i % baseDataCount
			key := fmt.Sprintf("base_%08d", readIndex)
			_, found, err := tree.Get([]byte(key))
			if err != nil {
				t.Errorf("混合读取失败: %v", err)
			} else if !found {
				t.Errorf("混合读取数据丢失: %s", key)
			}
			readCount++

		case 2: // 更新已有数据
			updateIndex := i % baseDataCount
			key := fmt.Sprintf("base_%08d", updateIndex)
			value := fmt.Sprintf("updated_base_value_%08d_%d", updateIndex, time.Now().UnixNano())
			err := tree.Put([]byte(key), []byte(value))
			if err != nil {
				t.Errorf("混合更新失败: %v", err)
			}
			updateCount++
		}
		if (i+1)%2000 == 0 {
			t.Logf("已完成 %d 次混合操作", i+1)
		}
	}

	mixedDuration := time.Since(mixedStart)
	mixedQPS := float64(operationCount) / mixedDuration.Seconds()
	t.Logf("✅ 混合操作完成: 写入 %d，读取 %d，更新 %d，总耗时: %v, QPS: %.2f",
		writeCount, readCount, updateCount, mixedDuration, mixedQPS)
}

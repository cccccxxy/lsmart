package util
func SharedPrefixLen(a, b []byte) int {
	var i int
	for ; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

// GetSeparatorBetween 返回结果 x，保证 a <= x < b. 使用方需要自行保证 a < b
func GetSeparatorBetween(a, b []byte) []byte {
	// 倘若 a 为空，则直接返回 b
	if len(a) == 0 {
		result := make([]byte, len(b))
		copy(result, b)
		return result
	}

	// 返回 a 即可
	return a
}

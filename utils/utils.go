package utils

import "strconv"

func StringToInt64(str string) (int64, error) {
	result, err := strconv.ParseInt(str, 10, 64)
	return result, err
}

package main

import (
	"time"

	"github.com/byte-power/gorich/task"
)

func main() {

	name1 := "once after 10 second"
	task.Once(name1, add, 20, 30).Delay(10 * time.Second)

	name2 := "every 10 second"
	task.Periodic(name2, minus, 100, 20).EverySeconds(10)

	name3 := "every 5 second"
	task.Periodic(name3, minus, 200, 80).EverySeconds(5)

	name4 := "every minute at 20"
	task.Periodic(name4, minus, 300, 20).EveryMinutes(1).AtSecondInMinute(20)

	// 开始调度
	task.Start()
}

func add(a, b int) int {
	return a + b
}

func minus(a, b int) int {
	return a - b
}

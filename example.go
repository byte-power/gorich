package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/byte-power/gorich/task"
)

func main() {
	name1 := "normal_job_once_10_seconds"
	task.Once(name1, normalFunction, 20, 30).Delay(10 * time.Second)

	name2 := "panic_job_every_10_seconds"
	task.Periodic(name2, panicFunction, 100, 20).EverySeconds(10)

	name3 := "err_job_every_5_seconds"
	task.Periodic(name3, errFunction, 200, 20).EverySeconds(5)

	name4 := "panic_job_every_minute_at_20_second"
	task.Periodic(name4, panicFunction, 300, 20).EveryMinutes(1).AtSecondInMinute(20)

	name5 := "job_stats_every_10_seconds"
	task.Periodic(name5, jobStats).EverySeconds(10)

	// 开始调度
	task.Start()
	// coordinator := task.NewCoordinator("coordinator1", "localhost:6379")
	// scheduled, t, err := coordinator.Coordinate("job1", time.Now(), time.Now().Add(10*time.Second))
	// fmt.Println(scheduled, t, err)
	// time.Sleep(5 * time.Second)
	// scheduled, t, err = coordinator.Coordinate("job1", time.Now(), time.Now().Add(10*time.Second))
	// fmt.Println(scheduled, t, err)
	// time.Sleep(10 * time.Second)
	// scheduled, t, err = coordinator.Coordinate("job1", time.Now(), time.Now().Add(10*time.Second))
	// fmt.Println(scheduled, t, err)
}

func normalFunction(a, b int) int {
	return a + b
}

func panicFunction(a, b int) {
	panic(errors.New("panic error"))
}

func errFunction() error {
	return errors.New("error happens in task")
}

func jobStats() {
	for name, stats := range task.JobStats() {
		fmt.Printf("job %s stat:\n", name)
		for _, stat := range stats {
			fmt.Println(stat.ToMap())
		}
	}
}

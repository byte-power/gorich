package task

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func emptyScheduler() {
	ClearJobs()
}
func TestOnceJob(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Once(name, func(a, b int) int { return a + b }, 10, 20)
	assert.True(t, job.IsRunnable(time.Now()))
	assert.Equal(t, 1, defaultScheduler.jobCount())

	job.Delay(2 * time.Second)
	assert.False(t, job.IsRunnable(time.Now()))
	time.Sleep(2 * time.Second)
	assert.True(t, job.IsRunnable(time.Now()))

	job.Run(time.Now())
	assert.False(t, job.IsRunnable(time.Now()))
	jobStats := job.Stats()
	assert.Len(t, jobStats, 1)
}

func TestPeroidicJob(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	runnable := job.IsRunnable(time.Now())
	assert.False(t, runnable)

}

func TestPeroidicJobNoAtTime(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	job.EveryMinutes(1)

	executeTime := time.Date(2020, time.November, 2, 10, 10, 30, 0, time.Local)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = time.Date(2020, time.November, 2, 10, 10, 0, 0, time.Local)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = time.Date(2020, time.November, 2, 10, 11, 0, 0, time.Local)
	assert.True(t, job.IsRunnable(executeTime))
}

func TestPeroidicJobEveryDay(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	hour := 10
	minute := 20
	second := 30
	job.EveryDays(2).AtHourInDay(hour, minute, second)

	executeTime := time.Date(2020, time.November, 2, 10, 10, 30, 0, time.Local)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = time.Date(2020, time.November, 2, hour, minute, second, 50, time.Local)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = executeTime.Add(1 * 24 * time.Hour)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = executeTime.Add(1 * 24 * time.Hour)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))
}

func TestPeroidicJobEveryHour(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	minute := 20
	second := 30
	job.EveryHours(2).AtMinuteInHour(minute, second)

	executeTime := time.Date(2020, time.November, 2, 1, minute, second+10, 50, time.Local)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = time.Date(2020, time.November, 2, 1, minute, second, 50, time.Local)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = executeTime.Add(1 * time.Hour)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = executeTime.Add(1 * time.Hour)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))
}

func TestPeroidicJobEveryMinute(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	second := 30
	job.EveryMinutes(2).AtSecondInMinute(second)

	executeTime := time.Date(2020, time.November, 2, 1, 0, second+10, 50, time.Local)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = time.Date(2020, time.November, 2, 1, 0, second, 50, time.Local)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = executeTime.Add(1 * time.Minute)
	assert.False(t, job.IsRunnable(executeTime))

	executeTime = executeTime.Add(1 * time.Minute)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))
}

func TestPeroidicJobEveryWeekday(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	currentTime := time.Now()
	hour := currentTime.Hour()
	minute := int(math.Mod(float64(currentTime.Minute())+10, 60))
	second := currentTime.Second()
	job.EveryMondays(2).AtHourInDay(hour, minute, second)

	// a Sunday
	executeTime := time.Date(2020, time.November, 1, hour, minute, second, 0, time.Local)
	assert.False(t, job.IsRunnable(executeTime))

	// a Monday
	executeTime = time.Date(2020, time.November, 2, hour, minute, second, 0, time.Local)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))

	// next monday
	executeTime = executeTime.Add(7 * 24 * time.Hour)
	assert.False(t, job.IsRunnable(executeTime))

	// next next monday
	executeTime = executeTime.Add(7 * 24 * time.Hour)
	assert.True(t, job.IsRunnable(executeTime))
	job.Run(executeTime)
	assert.False(t, job.IsRunnable(executeTime))
}

func TestPeriodicJobCoordinator(t *testing.T) {
	defer emptyScheduler()
	coordinator := NewCoordinatorFromRedis("coordinator1", "localhost:6379")

	scheduler1 := NewScheduler(10)
	scheduler2 := NewScheduler(10)
	sum := 0
	function := func(a int) { sum = sum + a }

	name := "test_job"
	job1 := scheduler1.AddPeriodicJob(name, function, 1).EverySeconds(5).Coordinate(coordinator)
	job2 := scheduler2.AddPeriodicJob(name, function, 1).EverySeconds(5).Coordinate(coordinator)

	currentTime := time.Now()
	assert.True(t, job1.IsRunnable(currentTime))
	assert.True(t, job2.IsRunnable(currentTime))
	job1.Run(currentTime)
	assert.True(t, currentTime.Truncate(time.Second).Equal(job1.scheduledTime))
	assert.False(t, job1.IsRunnable(currentTime))
	assert.False(t, job2.IsRunnable(currentTime))
	assert.True(t, currentTime.Truncate(time.Second).Equal(job2.scheduledTime))

	time.Sleep(5 * time.Second)
	currentTime = time.Now()
	assert.True(t, job1.IsRunnable(currentTime))
	assert.True(t, job2.IsRunnable(currentTime))
	job2.Run(currentTime)
	assert.False(t, job1.IsRunnable(currentTime))
	assert.False(t, job2.IsRunnable(currentTime))
	assert.True(t, currentTime.Truncate(time.Second).Equal(job1.scheduledTime))
	assert.True(t, currentTime.Truncate(time.Second).Equal(job2.scheduledTime))
}

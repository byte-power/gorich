package task

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func emptyScheduler() {
	RemoveAllJobs()
}

func TestJobCount(t *testing.T) {
	defer emptyScheduler()
	assert.Equal(t, 0, JobCount())
	Once("job1", func(a, b int) int { return a + b }, 10, 20)
	assert.Equal(t, 1, JobCount())
	Once("job2", func(a, b int) int { return a + b }, 10, 20)
	assert.Equal(t, 2, JobCount())
	Periodic("job3", func(a, b int) int { return a + b }, 100, 200)
	assert.Equal(t, 3, JobCount())
}

func TestJobAlreadyScheduled(t *testing.T) {
	defer emptyScheduler()
	job1 := Once("job1", func(a, b int) int { return a + b }, 10, 20)
	assert.False(t, job1.alreadyScheduled())
	job1.run(time.Now())
	assert.True(t, job1.alreadyScheduled())

	job2 := Periodic("job2", func(a, b int) int { return a + b }, 1, 2).EveryMinutes(1)
	assert.False(t, job2.alreadyScheduled())
	job2.run(time.Now().Truncate(time.Second))
	assert.True(t, job2.alreadyScheduled())
}

func TestOnceJobSchedulable(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Once(name, func(a, b int) int { return a + b }, 10, 20)
	schedulable, err := job.IsSchedulable(time.Now())
	assert.True(t, schedulable)
	assert.Nil(t, err)

	job.Delay(2 * time.Second)
	schedulable, err = job.IsSchedulable(time.Now())
	assert.False(t, schedulable)
	assert.Nil(t, err)
	time.Sleep(2 * time.Second)

	schedulable, err = job.IsSchedulable(time.Now())
	assert.Nil(t, err)
	assert.True(t, schedulable)

	job.run(time.Now())
	schedulable, err = job.IsSchedulable(time.Now())
	assert.False(t, schedulable)
	assert.Nil(t, err)
}

func TestOnceJobSchedulableWithCoordinator(t *testing.T) {
	defer emptyScheduler()
	name := "test_once_job_coordinate"
	coordinator := NewCoordinatorFromRedis("coordinator1", "localhost:6379")
	defer coordinator.removeCoordinatorKeyByJobName(name)

	scheduler1 := NewScheduler(10)
	scheduler2 := NewScheduler(10)
	function := func(a int) int { return a }

	job1 := scheduler1.AddRunOnceJob(name, function, 1).SetCoordinate(coordinator)
	job2 := scheduler2.AddRunOnceJob(name, function, 1).SetCoordinate(coordinator)

	currentTime := time.Now()
	isJob1Schedulable, err := job1.IsSchedulable(currentTime)
	assert.True(t, isJob1Schedulable)
	assert.Nil(t, err)

	isJob2Schedulable, err := job2.IsSchedulable(currentTime)
	assert.True(t, isJob2Schedulable)
	assert.Nil(t, err)
	job1.run(currentTime)

	isJob1Schedulable, err = job1.IsSchedulable(currentTime)
	assert.False(t, isJob1Schedulable)
	assert.Nil(t, err)

	isJob2Schedulable, err = job2.IsSchedulable(currentTime)
	assert.False(t, isJob2Schedulable)
	assert.Nil(t, err)

}

func TestPeroidicJobSchedulable(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job, err := Periodic(name, func(a, b int) int { return a + b }, 10, 20).EveryHours(2).AtMinuteInHour(35, 25)
	assert.Nil(t, err)
	scheduledTime, _ := time.Parse("2006010215:04:05", "2020010203:35:20")
	schedulable, err := job.IsSchedulable(scheduledTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	scheduledTime, _ = time.Parse("2006010215:04:05", "2020010203:35:25")
	schedulable, err = job.IsSchedulable(scheduledTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)

	job.run(scheduledTime)

	scheduledTime, _ = time.Parse("2006010215:04:05", "2020010204:35:25")
	schedulable, err = job.IsSchedulable(scheduledTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	scheduledTime, _ = time.Parse("2006010215:04:05", "2020010205:35:25")
	schedulable, err = job.IsSchedulable(scheduledTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)
}

func TestPeroidicJobTimeZone(t *testing.T) {
	defer emptyScheduler()
	// loc1 is UTC+8
	loc1, _ := time.LoadLocation("Asia/Shanghai")
	// loc2 is UTC
	loc2, _ := time.LoadLocation("UTC")
	// loc3 is UTC-6
	loc3 := time.FixedZone("UTC-6", -6*60*60)

	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	job.EveryDays(1).SetTimeZone(loc1).AtHourInDay(10, 0, 0)

	t1 := time.Date(2019, time.November, 2, 10, 0, 0, 0, loc1)
	schedulable, err := job.IsSchedulable(t1)
	assert.True(t, schedulable)
	assert.Nil(t, err)

	t2 := time.Date(2019, time.November, 2, 10, 0, 0, 0, loc2)
	schedulable, err = job.IsSchedulable(t2)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	t3 := time.Date(2019, time.November, 2, 2, 0, 0, 0, loc2)
	schedulable, err = job.IsSchedulable(t3)
	assert.True(t, schedulable)
	assert.Nil(t, err)

	t4 := time.Date(2019, time.November, 1, 20, 0, 0, 0, loc3)
	schedulable, err = job.IsSchedulable(t4)
	assert.True(t, schedulable)
	assert.Nil(t, err)
}

func TestPeroidicJobNoAtTime(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	job.EveryMinutes(1)

	executeTime := time.Date(2020, time.November, 2, 10, 10, 30, 0, time.Local)
	schedulable, err := job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = time.Date(2020, time.November, 2, 10, 10, 0, 0, time.Local)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)
	job.run(executeTime)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = time.Date(2020, time.November, 2, 10, 11, 0, 0, time.Local)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)
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
	schedulable, err := job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = time.Date(2020, time.November, 2, hour, minute, second, 50, time.Local)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	job.run(executeTime)

	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)

	executeTime = executeTime.Add(1 * 24 * time.Hour)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)

	executeTime = executeTime.Add(1 * 24 * time.Hour)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	job.run(executeTime)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
}

func TestPeroidicJobEveryHour(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	minute := 20
	second := 30
	job.EveryHours(2).AtMinuteInHour(minute, second)

	executeTime := time.Date(2020, time.November, 2, 1, minute, second+10, 50, time.Local)
	schedulable, err := job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = time.Date(2020, time.November, 2, 1, minute, second, 50, time.Local)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)
	job.run(executeTime)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = executeTime.Add(1 * time.Hour)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = executeTime.Add(1 * time.Hour)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)
	job.run(executeTime)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)
}

func TestPeroidicJobEveryMinute(t *testing.T) {
	defer emptyScheduler()
	name := "test_job"
	job := Periodic(name, func(a, b int) int { return a + b }, 10, 20)
	second := 30
	job.EveryMinutes(2).AtSecondInMinute(second)

	executeTime := time.Date(2020, time.November, 2, 1, 0, second+10, 50, time.Local)
	schedulable, err := job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = time.Date(2020, time.November, 2, 1, 0, second, 50, time.Local)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)

	job.run(executeTime)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = executeTime.Add(1 * time.Minute)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	executeTime = executeTime.Add(1 * time.Minute)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)
	job.run(executeTime)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)
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
	schedulable, err := job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	// a Monday
	executeTime = time.Date(2020, time.November, 2, hour, minute, second, 0, time.Local)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)

	job.run(executeTime)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	// next monday
	executeTime = executeTime.Add(7 * 24 * time.Hour)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)

	// next next monday
	executeTime = executeTime.Add(7 * 24 * time.Hour)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.True(t, schedulable)
	assert.Nil(t, err)
	job.run(executeTime)
	schedulable, err = job.IsSchedulable(executeTime)
	assert.False(t, schedulable)
	assert.Nil(t, err)
}

func TestPeriodicJobSchedulableWithCoordinator(t *testing.T) {
	coordinator := NewCoordinatorFromRedis("coordinator1", "localhost:6379")

	scheduler1 := NewScheduler(10)
	scheduler2 := NewScheduler(10)
	sum := 0
	function := func(a int) { sum = sum + a }

	name := "test_periodic_job_coordinate"
	defer coordinator.removeCoordinatorKeyByJobName(name)
	job1 := scheduler1.AddPeriodicJob(name, function, 1).EverySeconds(5).SetCoordinate(coordinator)
	job2 := scheduler2.AddPeriodicJob(name, function, 1).EverySeconds(5).SetCoordinate(coordinator)

	currentTime := time.Now()
	isJob1Schedulable, err := job1.IsSchedulable(currentTime)
	assert.True(t, isJob1Schedulable)
	assert.Nil(t, err)

	isJob2Schedulable, err := job2.IsSchedulable(currentTime)
	assert.True(t, isJob2Schedulable)
	assert.Nil(t, err)
	job1.run(currentTime)
	scheduledTime, err := job1.GetLatestScheduledTime()
	assert.True(t, currentTime.Truncate(time.Second).Equal(scheduledTime))
	assert.Nil(t, err)

	isJob1Schedulable, _ = job1.IsSchedulable(currentTime)
	assert.False(t, isJob1Schedulable)

	isJob2Schedulable, _ = job2.IsSchedulable(currentTime)
	scheduledTime, _ = job2.GetLatestScheduledTime()
	assert.False(t, isJob2Schedulable)
	assert.True(t, currentTime.Truncate(time.Second).Equal(scheduledTime))

	time.Sleep(5 * time.Second)
	currentTime = time.Now()
	isJob1Schedulable, _ = job1.IsSchedulable(currentTime)
	assert.True(t, isJob1Schedulable)
	isJob2Schedulable, _ = job2.IsSchedulable(currentTime)
	assert.True(t, isJob2Schedulable)
	job2.run(currentTime)

	isJob1Schedulable, _ = job1.IsSchedulable(currentTime)
	assert.False(t, isJob1Schedulable)
	isJob2Schedulable, _ = job2.IsSchedulable(currentTime)
	assert.False(t, isJob2Schedulable)
	scheduledTime, _ = job1.GetLatestScheduledTime()
	assert.True(t, currentTime.Truncate(time.Second).Equal(scheduledTime))
	scheduledTime, _ = job2.GetLatestScheduledTime()
	assert.True(t, currentTime.Truncate(time.Second).Equal(scheduledTime))
}

func TestCoordinate(t *testing.T) {
	coordinator := NewCoordinatorFromRedis("coordinator1", "localhost:6379")
	jobName := "job1"
	//defer coordinator.removeCoordinatorKey(jobName)
	layout := "2006-01-02:15:04:05"
	t1, _ := time.Parse(layout, "2012-03-04:05:06:01")
	stat, err := coordinator.coordinate(jobName, t1, 0)
	assert.Nil(t, err)
	assert.True(t, stat.schedulable)
	assert.True(t, t1.Truncate(time.Second).Equal(stat.scheduedAt))

	t2, _ := time.Parse(layout, "2012-03-04:05:06:02")
	stat, err = coordinator.coordinate(jobName, t2, 0)
	assert.False(t, stat.schedulable)
	assert.True(t, t1.Truncate(time.Second).Equal(stat.scheduedAt))
	assert.Nil(t, err)

	jobName2 := "job2"
	t3, _ := time.Parse(layout, "2012-03-04:05:16:01")
	stat, err = coordinator.coordinate(jobName2, t3, 2*time.Second)
	assert.Nil(t, err)
	assert.True(t, stat.schedulable)
	assert.True(t, t3.Truncate(time.Second).Equal(stat.scheduedAt))

	t4, _ := time.Parse(layout, "2012-03-04:05:16:02")
	stat, err = coordinator.coordinate(jobName2, t4, 2*time.Second)
	assert.Nil(t, err)
	assert.False(t, stat.schedulable)
	assert.True(t, t3.Truncate(time.Second).Equal(stat.scheduedAt))

	t5, _ := time.Parse(layout, "2012-03-04:05:16:03")
	stat, err = coordinator.coordinate(jobName2, t5, 2*time.Second)
	assert.Nil(t, err)
	assert.True(t, stat.schedulable)
	assert.True(t, t5.Truncate(time.Second).Equal(stat.scheduedAt))
}

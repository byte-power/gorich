package task_test

import (
	"fmt"
	"time"

	"github.com/byte-power/gorich/task"
)

func Example() {
	// will run immediately after scheduler starts
	task.Once("once_job1_name", sum, 10, 20)

	// will run 5 seconds after scheduler starts
	task.Once("once_job2_name", sum, 100, 200).Delay(5 * time.Second)

	// will run every 2 days at 10:20:30 (in local timezone by default) after scheduler starts
	_, err := task.Periodic("periodic_job3_name", sum, 20, 30).EveryDays(2).AtHourInDay(10, 20, 30)
	if err != nil {
		return
	}

	// will run every Friday at 10:20:30 (in Asia/Shanghai timezone) after scheduler starts
	tz, _ := time.LoadLocation("Asia/Shanghai")
	_, err = task.Periodic("periodic_job4_name", sum, 20, 40).EveryFridays(1).SetTimeZone(tz).AtHourInDay(10, 20, 30)
	if err != nil {
		return
	}

	// will run every 2 hours at 20:30
	_, err = task.Periodic("periodic_job5_name", sum, 10, 20).EveryHours(2).AtMinuteInHour(20, 30)
	if err != nil {
		return
	}

	// will run every 5 minutes at :20
	job, err := task.Periodic("periodic_job6_name", sum, 10, 20).EveryMinutes(5).AtSecondInMinute(20)
	if err != nil {
		return
	}

	// task.StartScheduler method will start the scheduler, it will loop to schedule runnable jobs.
	// you can add more jobs to schedule after call task.StartScheduler.
	// here, to show more use cases, start scheduler in a separate goroutine
	go task.StartScheduler()

	// return job's name: periodic_job6_name
	job.Name()

	// return job's running statistics
	jobStats := job.Stats()
	for _, stat := range jobStats {
		fmt.Println(stat.ToMap())
	}

	// return jobs's latest scheduled time, return time.Time{} if not scheduled yet.
	job.GetLatestScheduledTime()

	// return job count in scheduler
	task.JobCount()

	// handle all job stats
	allJobStats := task.JobStats()
	for jobName, jobStats := range allJobStats {
		fmt.Printf("job %s stat:\n", jobName)
		for _, stat := range jobStats {
			fmt.Println(stat.ToMap())
		}
	}
	// remove job by name in scheduler
	task.RemoveJob(job.Name())

	// remove all jobs in scheduler
	task.RemoveAllJobs()

	// Stop scheduler after 5 seconds
	// set argument to false indicates waiting all running jobs finish before return.
	time.Sleep(5 * time.Second)
	task.StopScheduler(false)
}

// Show how to coordinate and schedule multiple job instances with the same name.
func ExampleCoordinate() {
	coordinator := task.NewCoordinatorFromRedis("coordinator1", "localhost:6379")
	// with redis cluster, use:
	// task.NewCoordinatorFromRedisCluster("coordinator2", []string{"localhost:30000", "localhost:30001"})

	scheduler1 := task.NewScheduler(10)
	scheduler2 := task.NewScheduler(10)

	name := "coordinate_job"
	job1 := scheduler1.AddPeriodicJob(name, sum, 1, 2).EverySeconds(2).SetCoordinate(coordinator)
	job2 := scheduler2.AddPeriodicJob(name, sum, 3, 4).EverySeconds(2).SetCoordinate(coordinator)

	// job1 and job2 will coordinate, only one of them will be scheduled once every 2 seconds
	go scheduler1.Start()
	go scheduler2.Start()

	jobStats := job1.Stats()
	fmt.Println("job1 stats:")
	for _, stat := range jobStats {
		fmt.Println(stat.ToMap())
	}

	jobStats = job2.Stats()
	fmt.Println("job2 stats:")
	for _, stat := range jobStats {
		fmt.Println(stat.ToMap())
	}
	// stop schedulers after 10 seconds
	time.Sleep(10 * time.Second)

	scheduler1.Stop(false)
	scheduler2.Stop(false)
}

func sum(a, b int) int {
	return a + b
}

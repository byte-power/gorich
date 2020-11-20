# task package tutorial

task package implements a job scheduler. Jobs in scheduler can be scheduled to run periodically or once at specific time.

## 1. basic usages

You can add periodic jobs or run once jobs to the default scheduler, and then start to schedule.

Examples as below:

``` go
package main

import (
    "github.com/byte-power/gorich/task"
)

func main() {
    jobName1 := "once_job1_name"
    // Add jobName1 to the default scheduler, the job will run immediately after scheduler starts.
    task.Once(jobName1, sum, 10, 20)

    jobName2 := "once_job2_name"
    // Add jobName2 to the default scheduler, the job will run 5 seconds after scheduler starts.
    task.Once(jobName2, sum, 100, 200).Delay(5 * time.Second)

    jobName3 := "periodic_job3_name"
    // Add jobName3 to the default scheduler, the job will run every 2 days at 10:20:30 (in local timezone by default) after scheduler starts.
     _, err := task.Periodic(jobName3, sum, 20, 30).EveryDays(2).AtHourInDay(10, 20, 30)
     if err != nil {
        return
     }

    // Start the default scheduler.
    task.StartScheduler()
}

func sum(a, b int) int {
    return a + b
}

```

## 2. monitor job stats

You can also monitor the scheduled jobs via JobStats.

```go
package main

import (
    "github.com/byte-power/gorich/task"
)

func main() {
    jobName1 := "once_job1_name"
    // Add jobName1 to the default scheduler, the job will run immediately after scheduler starts.
    task.Once(jobName1, sum, 10, 20)

    jobName2 := "once_job2_name"
    // Add jobName2 to the default scheduler, the job will run 5 seconds after scheduler starts.
    task.Once(jobName2, sum, 100, 200).Delay(5 * time.Second)
    go monitorScheduler()
    task.StartScheduler()
}

func monitorScheduler() {
    // handle all job stats
    allJobStats := task.JobStats()
    for jobName, jobStats := range allJobStats {
        fmt.Printf("job %s stat:\n", jobName)
        for _, stat := range jobStats {
            fmt.Println(stat.ToMap())
        }
    }
}

func sum(a, b int) int {
    return a + b
}
```

## 3. job coordination

When running periodic jobs in multiple servers, you can use Coordinate to coordinate running  and avoid unnecessary running.

Notice that Coordinate use a lock that will unlock automatically 5 seconds later, so if the job running interval is less than 5 seconds, some runnings will not be allowed.

```go
package main

import (
    "github.com/byte-power/gorich/task"
)

func main () {
    coordinator := task.NewCoordinatorFromRedis("coordinator1", "localhost:6379")
    // with redis cluster, use:
    // task.NewCoordinatorFromRedisCluster("coordinator2", []string{"localhost:30000", "localhost:30001"})

    // starts two schedulers
    scheduler1 := task.NewScheduler(10)
    scheduler2 := task.NewScheduler(10)

    name := "coordinate_job"
    job1 := scheduler1.AddPeriodicJob(name, sum, 1, 2).EverySeconds(10).SetCoordinate(coordinator)
    job2 := scheduler2.AddPeriodicJob(name, sum, 3, 4).EverySeconds(10).SetCoordinate(coordinator)

    // job1 and job2 will coordinate, only one of them will be scheduled once every 10 seconds
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
```

See more examples [here](./example_test.go).

/*
Package task implements a task scheduler.
One task is mapped to a job and scheduled by a scheduler.

There are two scheduler strategies:

1) run only once at a specific time

2) run periodically
*/
package task

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/redis/go-redis/v9"
)

const (
	defaultConcurrentWorkerCount = 100
	maxStatsCount                = 100

	jobMaxExecutionDuration = 1 * time.Hour
)

var (
	// ErrRaceCondition means conflict condition happens when coordinated.
	ErrRaceCondition = errors.New("race condition when coordination")

	// ErrTimeRange means time range error.
	ErrTimeRange = errors.New("time range is invalid")

	// ErrJobTimeout means job's executed time exceeds `jobMaxExecutionDuration`(1 hour).
	ErrJobTimeout = errors.New("job is timeout")

	// ErrJobCronInvalid means job's cron expression is invalid.
	ErrJobCronInvalid = errors.New("job's cron is invalid")

	// ErrNotFunctionType means job's function is not function type.
	ErrNotFunctionType = errors.New("job's function is not function type")
	// ErrFunctionArityNotMatch means function arity(the number of parameters) does not match given arguments
	ErrFunctionArityNotMatch = errors.New("job's function arity does not match given arguments")
)

var defaultScheduler = NewScheduler(defaultConcurrentWorkerCount)

var contextTODO = context.TODO()

// Scheduler represents a scheduler.
type Scheduler struct {
	jobs       map[string]Job
	jobLock    sync.RWMutex
	workerPool *ants.Pool
	stop       chan bool
	started    int32
}

// Once adds a job to the default scheduler, and the job only run once.
func Once(name string, function interface{}, params ...interface{}) *OnceJob {
	return defaultScheduler.AddRunOnceJob(name, function, params...)
}

// Periodic add a job to the default scheduler, and the job run periodically.
func Periodic(name string, function interface{}, params ...interface{}) *PeriodicJob {
	return defaultScheduler.AddPeriodicJob(name, function, params...)
}

// StartScheduler starts the default scheduler.
func StartScheduler() {
	defaultScheduler.Start()
}

// StopScheduler stops the default scheduler.
func StopScheduler(force bool) {
	defaultScheduler.Stop(force)
}

// RemoveJob removes a job by name from the default scheduler.
func RemoveJob(name string) {
	defaultScheduler.RemoveJob(name)
}

// RemoveAllJobs removes all jobs from the default scheduler.
func RemoveAllJobs() {
	defaultScheduler.RemoveAllJobs()
}

// JobCount returns the number of jobs in the default scheduler.
func JobCount() int {
	return defaultScheduler.JobCount()
}

// JobStats returns all jobs' statistics in the default scheduler.
func JobStats() map[string][]JobStat {
	return defaultScheduler.JobStats()
}

// NewScheduler create a scheduler, and at most `workerCount` jobs can run concurrently in this scheduler.
func NewScheduler(workerCount int) Scheduler {
	pool, err := ants.NewPool(workerCount, ants.WithNonblocking(true))
	if err != nil {
		panic(err)
	}
	return Scheduler{
		jobs:       make(map[string]Job),
		jobLock:    sync.RWMutex{},
		workerPool: pool,
		stop:       make(chan bool),
	}
}

// JobCount returns the number of jobs in the current scheduler
func (scheduler *Scheduler) JobCount() int {
	return len(scheduler.jobs)
}

// AddPeriodicJob add a job to the current scheduler, and the job run periodically.
func (scheduler *Scheduler) AddPeriodicJob(name string, function interface{}, params ...interface{}) *PeriodicJob {
	job := NewPeriodicJob(name, function, params)
	scheduler.jobLock.Lock()
	defer scheduler.jobLock.Unlock()
	scheduler.jobs[name] = job
	return job
}

// AddRunOnceJob adds a job to the current scheduler, and the job only run once.
func (scheduler *Scheduler) AddRunOnceJob(name string, function interface{}, params ...interface{}) *OnceJob {
	job := NewOnceJob(name, function, params)
	scheduler.jobLock.Lock()
	defer scheduler.jobLock.Unlock()
	scheduler.jobs[name] = job
	return job
}

// RemoveJob removes a job by name from the current scheduler.
func (scheduler *Scheduler) RemoveJob(name string) {
	scheduler.jobLock.Lock()
	defer scheduler.jobLock.Unlock()
	delete(scheduler.jobs, name)
}

// RemoveAllJobs removes all jobs from the current scheduler.
func (scheduler *Scheduler) RemoveAllJobs() {
	scheduler.jobLock.Lock()
	defer scheduler.jobLock.Unlock()
	scheduler.jobs = make(map[string]Job)
}

// Start starts the current scheduler.
func (scheduler *Scheduler) Start() {
	if !atomic.CompareAndSwapInt32(&scheduler.started, 0, 1) {
		return
	}
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case tickerTime := <-ticker.C:
			scheduler.runJobs(tickerTime.Truncate(time.Second))
		case <-scheduler.stop:
			ticker.Stop()
			return
		}
	}
}

// Stop stops the current scheduler.
func (scheduler *Scheduler) Stop(force bool) {
	if !atomic.CompareAndSwapInt32(&scheduler.started, 1, 0) {
		return
	}
	scheduler.stop <- true
	if force {
		scheduler.workerPool.Release()
		return
	}
	for {
		runningCount := scheduler.runningJobCount()
		if runningCount > 0 {
			log.Printf("waiting %d jobs to finish...\n", runningCount)
		} else {
			scheduler.workerPool.Release()
			break
		}
		time.Sleep(time.Second)
	}
}

// StopWithTimeout stops the current scheduler, waiting running tasks at most `timeout` duration.
func (scheduler *Scheduler) StopWithTimeout(timeout time.Duration) {
	if !atomic.CompareAndSwapInt32(&scheduler.started, 1, 0) {
		return
	}
	scheduler.stop <- true
	if timeout <= 0 {
		scheduler.workerPool.Release()
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			scheduler.workerPool.Release()
			return
		default:
			runningCount := scheduler.runningJobCount()
			if runningCount > 0 {
				log.Printf("waiting %d jobs to finish...\n", runningCount)
			} else {
				scheduler.workerPool.Release()
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// JobStats returns all jobs' statistics in the current scheduler.
func (scheduler *Scheduler) JobStats() map[string][]JobStat {
	jobStats := make(map[string][]JobStat, len(scheduler.jobs))
	scheduler.jobLock.RLock()
	defer scheduler.jobLock.RUnlock()
	for name, job := range scheduler.jobs {
		jobStats[name] = job.Stats()
	}
	return jobStats
}

func (scheduler *Scheduler) runningJobCount() int {
	return scheduler.workerPool.Running()
}

func (scheduler *Scheduler) runJobs(t time.Time) {
	t = t.Truncate(time.Second)
	schedulableJobs := scheduler.getSchedulableJobs(t)
	for _, job := range schedulableJobs {
		function := func(job Job) func() {
			return func() {
				channel := make(chan JobStat)
				go func() {
					stat := job.run(t)
					channel <- stat
				}()
				select {
				case jobStat := <-channel:
					job.addStat(jobStat)
				case <-time.After(jobMaxExecutionDuration):
					jobStat := JobStat{
						IsSuccess:     false,
						Err:           ErrJobTimeout,
						ScheduledTime: t,
						RunDuration:   jobMaxExecutionDuration,
					}
					job.addStat(jobStat)
				}
			}
		}(job)
		if err := scheduler.workerPool.Submit(function); err != nil {
			jobStat := JobStat{IsSuccess: false, Err: err, ScheduledTime: t}
			job.addStat(jobStat)
		}
	}
}

func (scheduler *Scheduler) getSchedulableJobs(t time.Time) []Job {
	schedulableJobs := []Job{}
	scheduler.jobLock.RLock()
	defer scheduler.jobLock.RUnlock()
	for _, job := range scheduler.jobs {
		schedulable, err := job.IsSchedulable(t)
		if err != nil {
			jobStat := JobStat{IsSuccess: false, Err: err, ScheduledTime: t}
			job.addStat(jobStat)
			continue
		}
		if schedulable {
			schedulableJobs = append(schedulableJobs, job)
		}
	}
	return schedulableJobs
}

type coordinateError struct {
	err error
}

func (err *coordinateError) Error() string {
	if err.err == nil {
		return "coordinate error"
	}
	return fmt.Sprintf("coordinate error:%s", err.err.Error())
}

func (err *coordinateError) Unwrap() error {
	return err.err
}

func newCoordinateError(err error) error {
	return &coordinateError{err: err}
}

func IsCoordinateError(err error) bool {
	var e *coordinateError
	return errors.As(err, &e)
}

const (
	redisStandaloneMode = "standalone"
	redisClusterMode    = "cluster"
)

// Coordinator represents a coordinator.
type Coordinator struct {
	name               string
	redisMode          string
	redisClient        *redis.Client
	redisClusterClient *redis.ClusterClient
}

// NewCoordinatorFromRedis creates a coordinator based on standalone redis.
func NewCoordinatorFromRedis(name, address string) *Coordinator {
	redisClient := redis.NewClient(&redis.Options{Addr: address})
	return &Coordinator{name: name, redisMode: redisStandaloneMode, redisClient: redisClient}
}

// NewCoordinatorFromRedisCluster creates a coordinator based on redis cluster.
func NewCoordinatorFromRedisCluster(name string, addrs []string) *Coordinator {
	redisClusterClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	return &Coordinator{name: name, redisMode: redisClusterMode, redisClusterClient: redisClusterClient}
}

// Coordinate coordinates a job by name at scheduledTime.
func (coordinator *Coordinator) coordinate(jobName string, scheduledTime time.Time, scheduledInterval time.Duration) (scheduledStat, error) {
	key := coordinator.getCoordinatorKey(jobName)
	scheduledTime = scheduledTime.Truncate(time.Second)
	scheduledTs := scheduledTime.Unix()
	stat := scheduledStat{}
	coordinateFn := func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(
			contextTODO,
			func(pipeliner redis.Pipeliner) error {
				value, err := tx.Get(contextTODO, key).Result()
				var redisKeyDuration time.Duration
				if scheduledInterval == 0 {
					redisKeyDuration = 5 * time.Second
				} else {
					redisKeyDuration = coordinator.getCoordinatorKeyExpiration(scheduledInterval)
				}
				if err != nil {
					if err == redis.Nil {
						if _, err := pipeliner.Set(contextTODO, key, scheduledTs, redisKeyDuration).Result(); err != nil {
							return err
						}
						stat.scheduedAt = scheduledTime
						stat.schedulable = true
						return nil
					}
					return err
				}

				ts, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return err
				}

				if scheduledInterval == 0 {
					stat.schedulable = false
					stat.scheduedAt = time.Unix(ts, 0)
					return nil
				}

				if ts+int64(scheduledInterval/time.Second) <= scheduledTs {
					if _, err := pipeliner.Set(contextTODO, key, scheduledTs, redisKeyDuration).Result(); err != nil {
						return err
					}
					stat.scheduedAt = scheduledTime
					stat.schedulable = true
				} else {
					stat.scheduedAt = time.Unix(ts, 0)
					stat.schedulable = false
				}
				return nil
			})
		return err
	}
	var err error
	if coordinator.redisMode == redisStandaloneMode {
		err = coordinator.redisClient.Watch(contextTODO, coordinateFn, key)
	} else if coordinator.redisMode == redisClusterMode {
		err = coordinator.redisClusterClient.Watch(contextTODO, coordinateFn, key)
	}
	if err != nil {
		err = newCoordinateError(err)
	}
	return stat, err
}

func (coordinator *Coordinator) getCoordinatorKeyExpiration(scheduledInterval time.Duration) time.Duration {
	if scheduledInterval <= time.Second {
		return scheduledInterval
	}
	return scheduledInterval - time.Second
}

func (coordinator *Coordinator) isJobSchedulable(name string, scheduledTime time.Time, scheduledInterval time.Duration) (bool, error) {
	key := coordinator.getCoordinatorKey(name)
	scheduledTime = scheduledTime.Truncate(time.Second)
	scheduledTs := scheduledTime.Unix()
	var err error
	var value string
	if coordinator.redisMode == redisStandaloneMode {
		value, err = coordinator.redisClient.Get(contextTODO, key).Result()
	} else if coordinator.redisMode == redisClusterMode {
		value, err = coordinator.redisClusterClient.Get(contextTODO, key).Result()
	}
	if err != nil {
		if err == redis.Nil {
			return true, nil
		}
		return false, newCoordinateError(err)
	}
	if scheduledInterval == 0 {
		return false, nil
	}

	ts, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false, newCoordinateError(err)
	}
	if ts+int64(scheduledInterval/time.Second) <= scheduledTs {
		return true, nil
	}
	return false, nil
}

func (coordinator *Coordinator) removeCoordinatorKeyByJobName(jobName string) error {
	var client redis.Cmdable
	key := coordinator.getCoordinatorKey(jobName)
	if coordinator.redisMode == redisStandaloneMode {
		client = coordinator.redisClient
	} else {
		client = coordinator.redisClusterClient
	}
	_, err := client.Del(contextTODO, key).Result()
	return err
}

func (coordinator *Coordinator) getCoordinatorKey(jobName string) string {
	return fmt.Sprintf("gorich:task:%s:%s", coordinator.name, jobName)
}

func (coordinator *Coordinator) getScheduledTime(jobName string) (time.Time, error) {
	var client redis.Cmdable
	key := coordinator.getCoordinatorKey(jobName)
	if coordinator.redisMode == redisStandaloneMode {
		client = coordinator.redisClient
	} else {
		client = coordinator.redisClusterClient
	}
	value, err := client.Get(contextTODO, key).Result()
	if err != nil {
		if err == redis.Nil {
			return time.Time{}, nil
		}
		return time.Time{}, newCoordinateError(err)
	}
	ts, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, newCoordinateError(err)
	}
	return time.Unix(ts, 0), nil
}

// JobStat represents the running statistics of a job.
type JobStat struct {
	IsSuccess     bool
	Err           error
	RunDuration   time.Duration
	ScheduledTime time.Time
}

// ToMap converts a JobStat struct to a map.
func (stat JobStat) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"success":       stat.IsSuccess,
		"error":         stat.Err,
		"run_duration":  stat.RunDuration,
		"scheduledTime": stat.ScheduledTime,
	}
}

type scheduledStat struct {
	schedulable bool
	scheduedAt  time.Time
}

// Job represents a job
type Job interface {
	Name() string
	Stats() []JobStat
	GetLatestScheduledTime() (time.Time, error)
	IsSchedulable(time.Time) (bool, error)

	scheduledAt(time.Time)
	run(time.Time) JobStat
	addStat(stat JobStat)
}

type commonJob struct {
	name          string
	function      interface{}
	params        []interface{}
	scheduledTime time.Time
	jobStats      []JobStat
	jobStatLock   sync.Mutex
	coordinator   *Coordinator
}

// Name returns a job's name.
func (job *commonJob) Name() string {
	return job.name
}

// Stats returns a job's running statistics.
func (job *commonJob) Stats() []JobStat {
	job.jobStatLock.Lock()
	defer job.jobStatLock.Unlock()
	return job.jobStats
}

func (job *commonJob) addStat(stat JobStat) {
	job.jobStatLock.Lock()
	defer job.jobStatLock.Unlock()
	job.jobStats = append(job.jobStats, stat)
	if len(job.jobStats) > maxStatsCount {
		job.jobStats = job.jobStats[len(job.jobStats)-maxStatsCount:]
	}
}

// GetLatestScheduledTime returns job's latest scheduled time,
// returns time.Time{} if is not scheduled yet.
func (job *commonJob) GetLatestScheduledTime() (time.Time, error) {
	scheduledTime := job.scheduledTime
	if job.coordinator != nil {
		t, err := job.coordinator.getScheduledTime(job.name)
		if err != nil {
			return time.Time{}, err
		}
		scheduledTime = t
	}
	if scheduledTime.After(job.scheduledTime) {
		job.scheduledAt(scheduledTime)
	}
	return job.scheduledTime, nil
}

func (job *commonJob) alreadyScheduled() bool {
	return !job.scheduledTime.IsZero()
}

func (job *commonJob) scheduledAt(t time.Time) {
	job.scheduledTime = t.Truncate(time.Second)
}

func (job *commonJob) run(t time.Time, jobInterval time.Duration) JobStat {
	scheduledStat, err := job.coordinate(t, jobInterval)
	if err != nil {
		return JobStat{IsSuccess: false, Err: err, ScheduledTime: t}
	}
	if !scheduledStat.schedulable {
		job.scheduledAt(scheduledStat.scheduedAt)
		return JobStat{IsSuccess: false, Err: ErrRaceCondition, ScheduledTime: t}
	}

	t = t.Truncate(time.Second)
	startTime := time.Now()
	stat := runJobFunctionAndGetJobStat(job.function, job.params)
	stat.RunDuration = time.Since(startTime)
	stat.ScheduledTime = t
	job.scheduledAt(t)
	return stat
}

func (job *commonJob) setCoordinate(coordinator *Coordinator) {
	job.coordinator = coordinator
}

func (job *commonJob) coordinate(t time.Time, interval time.Duration) (scheduledStat, error) {
	scheduledTime := t.Truncate(time.Second)
	if job.coordinator == nil {
		return scheduledStat{schedulable: true, scheduedAt: scheduledTime}, nil
	}
	return job.coordinator.coordinate(job.name, scheduledTime, interval)
}

// OnceJob represents a job running only once.
type OnceJob struct {
	commonJob
	delay                 time.Duration
	expectedScheduledTime time.Time
}

// NewOnceJob creates a OnceJob.
func NewOnceJob(name string, function interface{}, params []interface{}) *OnceJob {
	job := &OnceJob{
		commonJob:             commonJob{name: name, function: function, params: params},
		delay:                 0,
		expectedScheduledTime: time.Now().Truncate(time.Second),
	}
	return job
}

// Delay set the delayed duration from now for the current OnceJob.
func (job *OnceJob) Delay(delay time.Duration) *OnceJob {
	job.delay = delay
	job.expectedScheduledTime = time.Now().Add(delay).Truncate(time.Second)
	return job
}

func (job *OnceJob) IsSchedulable(t time.Time) (bool, error) {
	t = t.Truncate(time.Second)
	var schedulable bool
	if !job.expectedScheduledTime.After(t) && !job.alreadyScheduled() {
		schedulable = true
	}
	if schedulable && job.coordinator != nil {
		ok, err := job.coordinator.isJobSchedulable(job.name, t, 0)
		if err != nil {
			return false, err
		}
		schedulable = ok
	}
	return schedulable, nil
}

func (job *OnceJob) Interval() time.Duration {
	return 0
}

// SetCoordinate sets coordinator for the current job.
func (job *OnceJob) SetCoordinate(coordinator *Coordinator) *OnceJob {
	job.commonJob.setCoordinate(coordinator)
	return job
}

func (job *OnceJob) run(t time.Time) JobStat {
	return job.commonJob.run(t, job.Interval())
}

// PeriodicJob represents a job running periodically.
type PeriodicJob struct {
	commonJob
	cron *cronExpression
}

// NewPeriodicJob creates a PeriodicJob.
func NewPeriodicJob(name string, function interface{}, params []interface{}) *PeriodicJob {
	return &PeriodicJob{
		commonJob: commonJob{name: name, function: function, params: params},
		cron:      &cronExpression{timezone: time.Local},
	}
}

func (job *PeriodicJob) Interval() time.Duration {
	return job.cron.intervalDuration()
}

func (job *PeriodicJob) IsSchedulable(t time.Time) (bool, error) {
	t = t.Truncate(time.Second)
	if !job.cron.isValid() {
		return false, ErrJobCronInvalid
	}
	schedulable := job.cron.isMatched(t)
	if !schedulable {
		return false, nil
	}
	// scheduledTime.IsZero == true if the job has not been sheduled yet.
	if job.alreadyScheduled() {
		if !t.Before(job.scheduledTime.Add(job.cron.intervalDuration())) {
			schedulable = true
		} else {
			schedulable = false
		}
	}
	if schedulable && job.coordinator != nil {
		ok, err := job.coordinator.isJobSchedulable(job.name, t, job.cron.intervalDuration())
		if err != nil {
			return false, err
		}
		schedulable = ok
	}
	return schedulable, nil
}

// SetCoordinate sets coordinator for the current job.
func (job *PeriodicJob) SetCoordinate(coordinator *Coordinator) *PeriodicJob {
	job.commonJob.setCoordinate(coordinator)
	return job
}

// EverySeconds sets running period in seconds for the current job.
func (job *PeriodicJob) EverySeconds(second int) *PeriodicJob {
	job.cron.intervalType = intervalSecond
	job.cron.interval = second
	return job
}

// EveryMinutes sets running period in minutes for the current job.
func (job *PeriodicJob) EveryMinutes(minute int) *PeriodicJob {
	job.cron.intervalType = intervalMinute
	job.cron.interval = minute
	return job
}

// EveryHours sets running period in hours for the current job.
func (job *PeriodicJob) EveryHours(hour int) *PeriodicJob {
	job.cron.intervalType = intervalHour
	job.cron.interval = hour
	return job
}

// EveryDays sets running period in days for the current job.
func (job *PeriodicJob) EveryDays(day int) *PeriodicJob {
	job.cron.intervalType = intervalDay
	job.cron.interval = day
	return job
}

// EveryMondays sets running period in Mondays for the current job.
func (job *PeriodicJob) EveryMondays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Monday
	return job
}

// EveryTuesdays sets running period in Tuesdays for the current job.
func (job *PeriodicJob) EveryTuesdays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Tuesday
	return job
}

// EveryWednesdays sets running period in Wednesdays for the current job.
func (job *PeriodicJob) EveryWednesdays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Wednesday
	return job
}

// EveryThursdays sets running period in Thursdays for the current job.
func (job *PeriodicJob) EveryThursdays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Thursday
	return job
}

// EveryFridays sets running period in Fridays for the current job.
func (job *PeriodicJob) EveryFridays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Friday
	return job
}

// EverySaturdays sets running period in Saturdays for the current job.
func (job *PeriodicJob) EverySaturdays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Saturday
	return job
}

// EverySundays sets running period in Sundays for the current job.
func (job *PeriodicJob) EverySundays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Sunday
	return job
}

// AtHourInDay sets hour time for jobs running periodically in days.
func (job *PeriodicJob) AtHourInDay(hour, minute, second int) (*PeriodicJob, error) {
	if err := job.cron.atHourInDay(hour, minute, second); err != nil {
		return nil, err
	}
	return job, nil
}

// AtMinuteInHour sets minute time for jobs running periodically in hours.
func (job *PeriodicJob) AtMinuteInHour(minute, second int) (*PeriodicJob, error) {
	if err := job.cron.atMinuteInHour(minute, second); err != nil {
		return nil, err
	}
	return job, nil
}

// AtSecondInMinute sets second time for jobs running periodically in minutes.
func (job *PeriodicJob) AtSecondInMinute(second int) (*PeriodicJob, error) {
	if err := job.cron.atSecondInMinute(second); err != nil {
		return nil, err
	}
	return job, nil
}

// SetTimeZone sets timezone for the current job.
func (job *PeriodicJob) SetTimeZone(tz *time.Location) *PeriodicJob {
	job.cron.setTimeZone(tz)
	return job
}

func (job *PeriodicJob) run(t time.Time) JobStat {
	return job.commonJob.run(t, job.Interval())
}

func interfaceToError(i interface{}) error {
	var err error
	switch v := i.(type) {
	case error:
		err = v
	case nil:
		err = nil
	default:
		err = fmt.Errorf("%+v", v)
	}
	return err
}

func runJobFunctionAndGetJobStat(function interface{}, params []interface{}) (stat JobStat) {
	stat.IsSuccess = true

	defer func() {
		if recovered := recover(); recovered != nil {
			stat.IsSuccess = false
			stat.Err = interfaceToError(recovered)
		}
	}()

	if reflect.TypeOf(function).Kind() != reflect.Func {
		stat.IsSuccess = false
		stat.Err = ErrNotFunctionType
	}
	f := reflect.ValueOf(function)
	if len(params) != f.Type().NumIn() {
		stat.IsSuccess = false
		stat.Err = ErrFunctionArityNotMatch
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	jobResults := f.Call(in)
	for _, result := range jobResults {
		if err, ok := result.Interface().(error); ok {
			stat.IsSuccess = false
			stat.Err = err
		}
	}
	return
}

type cronIntervalType string

const (
	intervalSecond cronIntervalType = "second"
	intervalMinute cronIntervalType = "minute"
	intervalHour   cronIntervalType = "hour"
	intervalDay    cronIntervalType = "day"
	intervalWeek   cronIntervalType = "week"
	intervalMonth  cronIntervalType = "month"
)

func (intervalType cronIntervalType) isZero() bool {
	return intervalType == ""
}

type cronExpression struct {
	interval     int
	intervalType cronIntervalType
	weekDay      time.Weekday
	at           time.Duration
	timezone     *time.Location
}

func (cron *cronExpression) isValid() bool {
	return (cron.interval != 0) && (!cron.intervalType.isZero())
}
func (cron *cronExpression) atHourInDay(hour, minute, second int) error {
	if !isValidHour(hour) || !isValidMinute(minute) && !isValidSecond(second) {
		return ErrTimeRange
	}
	cron.at = time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute + time.Duration(second)*time.Second
	return nil
}

func (cron *cronExpression) atMinuteInHour(minute, second int) error {
	if !isValidMinute(minute) || !isValidSecond(second) {
		return ErrTimeRange
	}
	cron.at = time.Duration(minute)*time.Minute + time.Duration(second)*time.Second
	return nil
}

func (cron *cronExpression) atSecondInMinute(second int) error {
	if !isValidSecond(second) {
		return ErrTimeRange
	}
	cron.at = time.Duration(second) * time.Second
	return nil
}

func (cron *cronExpression) setTimeZone(tz *time.Location) {
	cron.timezone = tz
}

func (cron *cronExpression) intervalDuration() time.Duration {
	var duration time.Duration
	switch cron.intervalType {
	case intervalWeek:
		duration = time.Duration(cron.interval) * 7 * 24 * time.Hour
	case intervalDay:
		duration = time.Duration(cron.interval) * 24 * time.Hour
	case intervalHour:
		duration = time.Duration(cron.interval) * time.Hour
	case intervalMinute:
		duration = time.Duration(cron.interval) * time.Minute
	case intervalSecond:
		duration = time.Duration(cron.interval) * time.Second
	}
	return duration
}

func (cron *cronExpression) isMatched(t time.Time) (matched bool) {
	at := cron.getAtTime(t)
	weekday := cron.getWeekday(t)
	if at == cron.at {
		if cron.intervalType == intervalWeek {
			if cron.weekDay == weekday {
				matched = true
			}
		} else {
			matched = true
		}
	}
	return
}

func (cron *cronExpression) getAtTime(t time.Time) time.Duration {
	t = t.In(cron.timezone)
	var at time.Duration
	switch cron.intervalType {
	case intervalWeek:
		at = time.Duration(t.Hour())*time.Hour +
			time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second
	case intervalDay:
		at = time.Duration(t.Hour())*time.Hour +
			time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second
	case intervalHour:
		at = time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second
	case intervalMinute:
		at = time.Duration(t.Second()) * time.Second
	case intervalSecond:
		at = time.Duration(0)
	}
	return at
}

func (cron *cronExpression) getWeekday(t time.Time) time.Weekday {
	return t.In(cron.timezone).Weekday()
}

func isValidHour(hour int) bool {
	return (hour >= 0) && (hour < 24)
}

func isValidMinute(minute int) bool {
	return (minute >= 0) && (minute < 60)
}

func isValidSecond(second int) bool {
	return (second >= 0) && (second < 60)
}

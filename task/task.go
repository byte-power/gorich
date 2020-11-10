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

	"github.com/go-redis/redis/v8"
	"github.com/panjf2000/ants/v2"
)

const (
	defaultConcurrentWorkerCount = 100
	defaultQueueSize             = 500
	maxStatsCount                = 100

	jobMaxExecutionDuration = 1 * time.Hour
)

var (
	ErrRaceCondition = errors.New("race condition when coordination")

	ErrTimeRange = errors.New("time range is invalid")

	ErrJobTimeout = errors.New("job is timeout")

	ErrNotFunctionType       = errors.New("job's function is not function type")
	ErrFunctionArityNotMatch = errors.New("job's function arity does not match given parameters")
)

var defaultScheduler = NewScheduler(defaultConcurrentWorkerCount)

type Scheduler struct {
	jobs       map[string]Job
	jobLock    sync.RWMutex
	workerPool *ants.Pool
	stop       chan bool
	started    int32
}

func Once(name string, function interface{}, params ...interface{}) *OnceJob {
	return defaultScheduler.AddRunOnceJob(name, function, params...)
}

func Periodic(name string, function interface{}, params ...interface{}) *PeriodicJob {
	return defaultScheduler.AddPeriodicJob(name, function, params...)
}

func StartScheduler() {
	defaultScheduler.Start()
}

func StopScheduler(force bool) {
	defaultScheduler.Stop(force)
}

func RemoveJob(name string) {
	defaultScheduler.RemoveJob(name)
}

func RemoveAllJobs() {
	defaultScheduler.ClearJobs()
}

func JobCount() int {
	return defaultScheduler.JobCount()
}

func NewScheduler(workerCount int) Scheduler {
	pool, err := ants.NewPool(workerCount, ants.WithNonblocking(true))
	if err != nil {
		panic(err)
	}
	return Scheduler{
		jobs:       make(map[string]Job, 0),
		jobLock:    sync.RWMutex{},
		workerPool: pool,
		stop:       make(chan bool),
	}
}

func (scheduler *Scheduler) JobCount() int {
	return len(scheduler.jobs)
}

func (scheduler *Scheduler) AddPeriodicJob(name string, function interface{}, params ...interface{}) *PeriodicJob {
	job := NewPeriodicJob(name, function, params)
	scheduler.jobLock.Lock()
	defer scheduler.jobLock.Unlock()
	scheduler.jobs[name] = job
	return job
}

func (scheduler *Scheduler) AddRunOnceJob(name string, function interface{}, params ...interface{}) *OnceJob {
	job := NewOnceJob(name, function, params)
	scheduler.jobLock.Lock()
	defer scheduler.jobLock.Unlock()
	scheduler.jobs[name] = job
	return job
}

func (scheduler *Scheduler) getRunnableJobs(t time.Time) []Job {
	runnableJobs := []Job{}
	scheduler.jobLock.RLock()
	defer scheduler.jobLock.RUnlock()
	for _, job := range scheduler.jobs {
		if job.isRunnable(t) {
			runnableJobs = append(runnableJobs, job)
		}
	}
	return runnableJobs
}

func (scheduler *Scheduler) RemoveJob(name string) {
	scheduler.jobLock.Lock()
	defer scheduler.jobLock.Unlock()
	delete(scheduler.jobs, name)
}

func (scheduler *Scheduler) ClearJobs() {
	scheduler.jobLock.Lock()
	defer scheduler.jobLock.Unlock()
	scheduler.jobs = make(map[string]Job, 0)
}

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

func (scheduler *Scheduler) runningJobCount() int {
	return scheduler.workerPool.Running()
}

func (scheduler *Scheduler) runJobs(t time.Time) {
	runnableJobs := scheduler.getRunnableJobs(t)
	for _, job := range runnableJobs {
		function := func() {
			channel := make(chan bool, 1)
			go func() {
				job.run(t)
				channel <- true
			}()
			select {
			case <-channel:
				return
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
		if err := scheduler.workerPool.Submit(function); err != nil {
			jobStat := JobStat{IsSuccess: false, Err: err, ScheduledTime: t}
			job.addStat(jobStat)
		}
	}
}

func (scheduler *Scheduler) JobStats() map[string][]JobStat {
	jobStats := make(map[string][]JobStat, len(scheduler.jobs))
	scheduler.jobLock.RLock()
	defer scheduler.jobLock.RUnlock()
	for name, job := range scheduler.jobs {
		jobStats[name] = job.Stats()
	}
	return jobStats
}

func newCoordinateError(err error) error {
	return fmt.Errorf("coordinate error:%w", err)
}

const (
	redisStandaloneMode = "standalone"
	redisClusterMode    = "cluster"
)

type Coordinator struct {
	name               string
	redisMode          string
	redisClient        *redis.Client
	redisClusterClient *redis.ClusterClient
}

func NewCoordinatorFromRedis(name, address string) *Coordinator {
	redisClient := redis.NewClient(&redis.Options{Addr: address})
	return &Coordinator{name: name, redisMode: redisStandaloneMode, redisClient: redisClient}
}

func NewCoordinatorFromRedisCluster(name string, addrs []string) *Coordinator {
	redisClusterClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	return &Coordinator{name: name, redisMode: redisClusterMode, redisClusterClient: redisClusterClient}
}

func (coordinator *Coordinator) Coordinate(name string, scheduledTime time.Time) (ok bool, err error) {
	key := coordinator.getCoordinatorKey(name)
	scheduledTs := scheduledTime.Truncate(time.Second).Unix()
	if coordinator.redisMode == redisStandaloneMode {
		ok, err = coordinator.redisClient.SetNX(context.Background(), key, scheduledTs, 5*time.Second).Result()
	} else if coordinator.redisMode == redisClusterMode {
		ok, err = coordinator.redisClusterClient.SetNX(context.Background(), key, scheduledTs, 5*time.Second).Result()
	}
	if err != nil {
		err = newCoordinateError(err)
	}
	return
}

func (coordinator *Coordinator) getCoordinatorKey(jobName string) string {
	return fmt.Sprintf("gorich:task:%s:%s", coordinator.name, jobName)
}

func (coordinator *Coordinator) checkRunnableAndGetLastScheduledTime(name string) (isRunnable bool, scheduledTime time.Time, err error) {
	key := coordinator.getCoordinatorKey(name)
	var value string
	if coordinator.redisMode == redisStandaloneMode {
		value, err = coordinator.redisClient.Get(context.Background(), key).Result()
	} else if coordinator.redisMode == redisClusterMode {
		value, err = coordinator.redisClusterClient.Get(context.Background(), key).Result()
	}
	if err == nil {
		if ts, convErr := strconv.ParseInt(value, 10, 64); convErr != nil {
			err = newCoordinateError(convErr)
		} else {
			scheduledTime = time.Unix(ts, 0)
		}
	}
	if err == redis.Nil {
		err = nil
		isRunnable = true
	}
	if err != nil {
		err = newCoordinateError(err)
	}
	return
}

func JobStats() map[string][]JobStat {
	return defaultScheduler.JobStats()
}

type JobStat struct {
	IsSuccess     bool
	Err           error
	RunDuration   time.Duration
	ScheduledTime time.Time
}

func (stat JobStat) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"success":       stat.IsSuccess,
		"error":         stat.Err,
		"run_duration":  stat.RunDuration,
		"scheduledTime": stat.ScheduledTime,
	}
}

type Job interface {
	Name() string
	Stats() []JobStat
	GetLatestScheduledTime() time.Time

	isRunnable(time.Time) bool
	scheduledAt(time.Time)
	run(time.Time)
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

func (job *commonJob) Name() string {
	return job.name
}

func (job *commonJob) Stats() []JobStat {
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

func (job *commonJob) GetLatestScheduledTime() time.Time {
	return job.scheduledTime
}

func (job *commonJob) run(t time.Time) {
	t = t.Truncate(time.Second)
	startTime := time.Now()
	stat := runJobFunctionAndGetJobStat(job.function, job.params)
	stat.RunDuration = time.Now().Sub(startTime)
	stat.ScheduledTime = t
	job.addStat(stat)
}

func (job *commonJob) setCoordinate(coordinator *Coordinator) {
	job.coordinator = coordinator
}

func (job *commonJob) coordinate(t time.Time) bool {
	if job.coordinator == nil {
		return true
	}
	scheduledTime := t.Truncate(time.Second)
	canBeScheduled, err := job.coordinator.Coordinate(job.name, scheduledTime)
	if err != nil {
		jobStat := JobStat{IsSuccess: false, Err: err, ScheduledTime: scheduledTime}
		job.addStat(jobStat)
		return false
	}
	if !canBeScheduled {
		jobStat := JobStat{IsSuccess: false, Err: ErrRaceCondition, ScheduledTime: scheduledTime}
		job.addStat(jobStat)
		return false
	}
	return true
}

type OnceJob struct {
	commonJob
	delay                 time.Duration
	scheduled             bool
	expectedScheduledTime time.Time
	jobStatLock           sync.Mutex
}

func NewOnceJob(name string, function interface{}, params []interface{}) *OnceJob {
	job := &OnceJob{
		commonJob:             commonJob{name: name, function: function, params: params},
		delay:                 0,
		expectedScheduledTime: time.Now().Truncate(time.Second),
	}
	return job
}

func (job *OnceJob) Delay(delay time.Duration) *OnceJob {
	job.delay = delay
	job.expectedScheduledTime = time.Now().Add(delay).Truncate(time.Second)
	return job
}

func (job *OnceJob) isRunnable(t time.Time) bool {
	var runnable bool
	if !job.expectedScheduledTime.After(t) && !job.scheduled {
		runnable = true
	}
	if runnable && job.coordinator != nil {
		ok, scheduledTime, err := job.coordinator.checkRunnableAndGetLastScheduledTime(job.name)
		if err != nil {
			jobStat := JobStat{IsSuccess: false, Err: err, ScheduledTime: t}
			job.addStat(jobStat)
			runnable = false
		} else {
			runnable = ok
			if !ok && !scheduledTime.IsZero() {
				job.scheduledAt(scheduledTime)
			}
		}
	}
	return runnable
}

func (job *OnceJob) SetCoordinate(coordinator *Coordinator) *OnceJob {
	job.commonJob.setCoordinate(coordinator)
	return job
}

func (job *OnceJob) scheduledAt(t time.Time) {
	job.scheduledTime = t.Truncate(time.Second)
	job.scheduled = true
}

func (job *OnceJob) run(t time.Time) {
	log.Printf("run job: %s\n", job.name)
	if job.coordinate(t) {
		job.scheduledAt(t)
		job.commonJob.run(t)
	}
}

type PeriodicJob struct {
	commonJob
	cron *cronExpression
}

func NewPeriodicJob(name string, function interface{}, params []interface{}) *PeriodicJob {
	return &PeriodicJob{
		commonJob: commonJob{name: name, function: function, params: params},
		cron:      &cronExpression{timezone: time.Local},
	}
}

func (job *PeriodicJob) isRunnable(t time.Time) bool {
	if !job.cron.isValid() {
		return false
	}
	runnable := job.cron.isMatched(t)
	if !runnable {
		return false
	}
	// scheduledTime.IsZero == true if the job has not been sheduled yet.
	if !job.scheduledTime.IsZero() {
		scheduledTime := job.scheduledTime.Truncate(time.Second)
		roundCurrentTime := t.Truncate(time.Second)
		if !roundCurrentTime.Before(scheduledTime.Add(job.cron.intervalDuration())) {
			runnable = true
		} else {
			runnable = false
		}
	}
	if runnable && job.coordinator != nil {
		ok, scheduledTime, err := job.coordinator.checkRunnableAndGetLastScheduledTime(job.name)
		if err != nil {
			jobStat := JobStat{IsSuccess: false, Err: err, ScheduledTime: t}
			job.addStat(jobStat)
			runnable = false
		} else {
			runnable = ok
			if !ok && !scheduledTime.IsZero() {
				job.scheduledAt(scheduledTime)
			}
		}
	}
	return runnable
}

func (job *PeriodicJob) scheduledAt(t time.Time) {
	job.scheduledTime = t.Truncate(time.Second)
}

func (job *PeriodicJob) SetCoordinate(coordinator *Coordinator) *PeriodicJob {
	job.commonJob.setCoordinate(coordinator)
	return job
}

func (job *PeriodicJob) EverySeconds(second int) *PeriodicJob {
	job.cron.intervalType = intervalSecond
	job.cron.interval = second
	return job
}

func (job *PeriodicJob) EveryMinutes(minute int) *PeriodicJob {
	job.cron.intervalType = intervalMinute
	job.cron.interval = minute
	return job
}

func (job *PeriodicJob) EveryHours(hour int) *PeriodicJob {
	job.cron.intervalType = intervalHour
	job.cron.interval = hour
	return job
}

func (job *PeriodicJob) EveryDays(day int) *PeriodicJob {
	job.cron.intervalType = intervalDay
	job.cron.interval = day
	return job
}

func (job *PeriodicJob) EveryMondays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Monday
	return job
}

func (job *PeriodicJob) EveryTuesdays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Tuesday
	return job
}

func (job *PeriodicJob) EveryWednesdays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Wednesday
	return job
}

func (job *PeriodicJob) EveryThursdays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Thursday
	return job
}

func (job *PeriodicJob) EveryFridays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Friday
	return job
}

func (job *PeriodicJob) EverySaturdays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Saturday
	return job
}

func (job *PeriodicJob) EverySundays(week int) *PeriodicJob {
	job.cron.intervalType = intervalWeek
	job.cron.interval = week
	job.cron.weekDay = time.Sunday
	return job
}

func (job *PeriodicJob) AtHourInDay(hour, minute, second int) (*PeriodicJob, error) {
	if err := job.cron.AtHourInDay(hour, minute, second); err != nil {
		return nil, err
	}
	return job, nil
}

func (job *PeriodicJob) AtMinuteInHour(minute, second int) (*PeriodicJob, error) {
	if err := job.cron.atMinuteInHour(minute, second); err != nil {
		return nil, err
	}
	return job, nil
}

func (job *PeriodicJob) AtSecondInMinute(second int) (*PeriodicJob, error) {
	if err := job.cron.atSecondInMinute(second); err != nil {
		return nil, err
	}
	return job, nil
}

func (job *PeriodicJob) SetTimeZone(tz *time.Location) *PeriodicJob {
	job.cron.setTimeZone(tz)
	return job
}

func (job *PeriodicJob) run(t time.Time) {
	log.Printf("run job: %s\n", job.name)
	if job.coordinate(t) {
		job.scheduledAt(t)
		job.commonJob.run(t)
	}
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

type IntervalType string

const (
	intervalSecond IntervalType = "second"
	intervalMinute IntervalType = "minute"
	intervalHour   IntervalType = "hour"
	intervalDay    IntervalType = "day"
	intervalWeek   IntervalType = "week"
	intervalMonth  IntervalType = "month"
)

func (intervalType IntervalType) isZero() bool {
	return intervalType == ""
}

type cronExpression struct {
	interval     int
	intervalType IntervalType
	weekDay      time.Weekday
	at           time.Duration
	timezone     *time.Location
}

func newCron(interval int, intervalType IntervalType, at time.Duration, timezone *time.Location) *cronExpression {
	// interval less or euqual to 0 will be set to 1.
	if interval <= 0 {
		interval = 1
	}
	return &cronExpression{
		interval:     interval,
		intervalType: intervalType,
		at:           at,
		timezone:     timezone,
	}
}

func (cron *cronExpression) isValid() bool {
	return (cron.interval != 0) && (!cron.intervalType.isZero())
}
func (cron *cronExpression) AtHourInDay(hour, minute, second int) error {
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

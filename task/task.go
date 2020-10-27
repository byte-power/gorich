package task

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

const (
	defaultConcurrentWorkerCount = 100
	defaultQueueSize             = 500
)

var defaultScheduler = NewScheduler(defaultQueueSize, defaultConcurrentWorkerCount)

type Scheduler struct {
	jobs                  map[string]Job
	concurrentWorkerCount int
	queue                 chan Job
}

func NewScheduler(queueSize, workerCount int) Scheduler {
	return Scheduler{
		jobs:                  make(map[string]Job, 0),
		concurrentWorkerCount: workerCount,
		queue:                 make(chan Job, queueSize),
	}
}

func (scheduler *Scheduler) jobCount() int {
	return len(scheduler.jobs)
}

func (scheduler *Scheduler) AddPeriodicJob(name string, function interface{}, params ...interface{}) *PeriodicJob {
	job := &PeriodicJob{
		name:     name,
		function: function,
		params:   params,
		cron:     &Cron{timezone: time.Local},
	}
	scheduler.jobs[name] = job
	return job
}

func (scheduler *Scheduler) AddRunOnceJob(name string, function interface{}, params ...interface{}) *OnceJob {
	job := &OnceJob{
		name:     name,
		function: function,
		params:   params,
		delay:    0,
	}
	scheduler.jobs[name] = job
	return job
}

func (scheduler *Scheduler) getRunnableJobs(t time.Time) []Job {
	runnableJobs := []Job{}
	for _, job := range scheduler.jobs {
		if job.IsRunnable(t) {
			runnableJobs = append(runnableJobs, job)
		}
	}
	return runnableJobs
}

func (scheduler *Scheduler) Start() {
	fmt.Println("start scheduler")
	go scheduler.startWorkers(scheduler.concurrentWorkerCount)
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case tickerTime := <-ticker.C:
			scheduler.QueueRunnableJobs(tickerTime)
		}
		fmt.Println("finish for in Start")
	}
}

func (scheduler *Scheduler) QueueRunnableJobs(t time.Time) {
	runnableJobs := scheduler.getRunnableJobs(t)
	for _, job := range runnableJobs {
		scheduler.queue <- job
		job.ScheduledAt(t)
	}
}

func (scheduler *Scheduler) startWorkers(count int) {
	fmt.Println("start workers")
	for {
		select {
		case job := <-scheduler.queue:
			go job.Run()
		}
		fmt.Println("finish for loop in startWorkers")
	}
}

func Once(name string, function interface{}, params ...interface{}) *OnceJob {
	return defaultScheduler.AddRunOnceJob(name, function, params)
}

func Periodic(name string, function interface{}, params ...interface{}) *PeriodicJob {
	return defaultScheduler.AddPeriodicJob(name, function, params)
}

type Job interface {
	IsRunnable(time.Time) bool
	ScheduledAt(time.Time)
	Run()
}

type OnceJob struct {
	name          string
	function      interface{}
	params        []interface{}
	delay         time.Duration
	scheduledTime time.Time
}

func (job *OnceJob) Delay(delay time.Duration) *OnceJob {
	job.delay = delay
	return job
}

func (job *OnceJob) IsRunnable(t time.Time) bool {
	return true
}

func (job *OnceJob) ScheduledAt(t time.Time) {
	job.scheduledTime = t
}

func (job *OnceJob) Run() {
	callJobFuncWithParams(job.function, job.params)
}

type PeriodicJob struct {
	name          string
	function      interface{}
	params        []interface{}
	cron          *Cron
	scheduledTime time.Time
}

func (job *PeriodicJob) IsRunnable(t time.Time) bool {
	at := getAtTime(job.cron.intervalType, t)
	var isReady bool
	// scheduledTime.IsZero == true if the job has not been sheduled yet.
	if job.scheduledTime.IsZero() {
		if (job.cron.at == 0) || (at == job.cron.at) {
			isReady = true
		} else {
			isReady = false
		}
	} else {
		if job.scheduledTime.Add(job.cron.Duration()) == t {
			isReady = true
		} else {
			isReady = false
		}
	}
	return isReady
}

func (job *PeriodicJob) ScheduledAt(t time.Time) {
	job.scheduledTime = t
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

func getAtTime(intervalType IntervalType, t time.Time) time.Duration {
	var at time.Duration
	switch intervalType {
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

func (job *PeriodicJob) Run() {
	callJobFuncWithParams(job.function, job.params)
}

type Queue interface{}

type Worker interface{}

func callJobFuncWithParams(jobFunc interface{}, params []interface{}) ([]reflect.Value, error) {
	f := reflect.ValueOf(jobFunc)
	if len(params) != f.Type().NumIn() {
		return nil, errors.New("error")
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	return f.Call(in), nil
}

// scheduler = NewScheduler
// scheduler.Every(time.Duration(100days)).Do(job)
// scheduler.EveryDay(2).At().Do(job)
// scheduler.EveryMonday()
// scheduler.EveryMonday(2).At().Do(job)
// scheduler.AddJob(job, task.Every(time.Duration).At())

// EveryMinutes(2).AtSeconds(second)
// EveryHours(2).AtMinute(minute)
// EveryDays(2).AtHour(hour).Withtimezone
// EveryWeek(2).AtWeekday(Monday).AtHour(hour)
// EveryMonth(2).AtDateOfMonth().AtHour(hour)
type IntervalType string

const (
	intervalSecond IntervalType = "second"
	intervalMinute IntervalType = "minute"
	intervalHour   IntervalType = "hour"
	intervalDay    IntervalType = "day"
	intervalWeek   IntervalType = "week"
	intervalMonth  IntervalType = "month"
)

type Cron struct {
	interval     int
	intervalType IntervalType
	at           time.Duration
	timezone     *time.Location
}

func newCron(interval int, intervalType IntervalType, at time.Duration, timezone *time.Location) *Cron {
	// interval less or euqual to 0 will be set to 1.
	if interval <= 0 {
		interval = 1
	}
	return &Cron{
		interval:     interval,
		intervalType: intervalType,
		at:           at,
		timezone:     timezone,
	}
}

func EverySeconds(second int) *Cron {
	return newCron(second, intervalSecond, 0, time.Local)
}

func EveryMinutes(minute int) *Cron {
	return newCron(minute, intervalMinute, 0, time.Local)
}

func EveryHours(hour int) *Cron {
	return newCron(hour, intervalHour, 0, time.Local)
}

func EveryDays(day int) *Cron {
	return newCron(day, intervalDay, 0, time.Local)
}

var ErrTimeRange = errors.New("time range is invalid")

func (cron *Cron) AtHourInDay(hour, minute, second int) (*Cron, error) {
	if !isValidHour(hour) || !isValidMinute(minute) && !isValidSecond(second) {
		return nil, ErrTimeRange
	}
	cron.at = time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute + time.Duration(second)*time.Second
	return cron, nil
}

func (cron *Cron) AtMinuteInHour(minute, second int) (*Cron, error) {
	if !isValidMinute(minute) || !isValidSecond(second) {
		return nil, ErrTimeRange
	}
	cron.at = time.Duration(minute)*time.Minute + time.Duration(second)*time.Second
	return cron, nil
}

func (cron *Cron) AtSecondInMinute(second int) error {
	if !isValidSecond(second) {
		return ErrTimeRange
	}
	cron.at = time.Duration(second) * time.Second
	return nil
}

func (cron *Cron) Duration() time.Duration {
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

func isValidHour(hour int) bool {
	return (hour > 0) && (hour < 24)
}

func isValidMinute(minute int) bool {
	return (minute > 0) && (minute < 60)
}

func isValidSecond(second int) bool {
	return (second > 0) && (second < 60)
}

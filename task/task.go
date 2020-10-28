package task

import (
	"errors"
	"log"
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
	job := NewOnceJob(name, function, params)
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
	go scheduler.startWorkers()
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case tickerTime := <-ticker.C:
			scheduler.QueueRunnableJobs(tickerTime)
		}
	}
}

func (scheduler *Scheduler) QueueRunnableJobs(t time.Time) {
	runnableJobs := scheduler.getRunnableJobs(t)
	for _, job := range runnableJobs {
		log.Printf("queue runnable job:%s\n", job.Name())
		scheduler.queue <- job
		job.ScheduledAt(t)
	}
}

func (scheduler *Scheduler) startWorkers() {
	for {
		select {
		case job := <-scheduler.queue:
			log.Printf("run job:%s\n", job.Name())
			go job.Run()
		}
	}
}

func Once(name string, function interface{}, params ...interface{}) *OnceJob {
	return defaultScheduler.AddRunOnceJob(name, function, params)
}

func Periodic(name string, function interface{}, params ...interface{}) *PeriodicJob {
	return defaultScheduler.AddPeriodicJob(name, function, params)
}

func Start() {
	defaultScheduler.Start()
}

type Job interface {
	Name() string
	IsRunnable(time.Time) bool
	ScheduledAt(time.Time)
	Run()
}

type OnceJob struct {
	name          string
	function      interface{}
	params        []interface{}
	delay         time.Duration
	runnable      bool
	timer         *time.Timer
	scheduled     bool
	scheduledTime time.Time
}

func NewOnceJob(name string, function interface{}, params ...interface{}) *OnceJob {
	job := &OnceJob{
		name:     name,
		function: function,
		params:   params,
		delay:    0,
		runnable: true,
	}
	return job
}

func (job *OnceJob) Name() string {
	return job.name
}

func (job *OnceJob) Delay(delay time.Duration) *OnceJob {
	job.delay = delay
	job.runnable = false
	if job.timer != nil {
		job.timer.Stop()
	}
	go job.waitUntilRunnable()
	return job
}

func (job *OnceJob) waitUntilRunnable() {
	job.timer = time.NewTimer(job.delay)
	<-job.timer.C
	job.runnable = true
}

func (job *OnceJob) IsRunnable(t time.Time) bool {
	return job.runnable && !job.scheduled
}

func (job *OnceJob) ScheduledAt(t time.Time) {
	job.scheduledTime = t
	job.scheduled = true
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

func (job *PeriodicJob) Name() string {
	return job.name
}

func (job *PeriodicJob) IsRunnable(t time.Time) bool {
	at := getAtTime(job.cron.intervalType, t)
	var isRunnable bool
	// scheduledTime.IsZero == true if the job has not been sheduled yet.
	if job.scheduledTime.IsZero() {
		if (job.cron.at == 0) || (at == job.cron.at) {
			isRunnable = true
		} else {
			isRunnable = false
		}
	} else {
		roundScheduledTime := job.scheduledTime.Truncate(time.Second)
		roundCurrentTime := t.Truncate(time.Second)
		if !roundCurrentTime.Before(roundScheduledTime.Add(job.cron.IntervalDuration())) {
			isRunnable = true
		} else {
			isRunnable = false
		}
	}
	return isRunnable
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

func (job *PeriodicJob) AtHourInDay(hour, minute, second int) (*PeriodicJob, error) {
	if err := job.cron.AtHourInDay(hour, minute, second); err != nil {
		return nil, err
	}
	return job, nil
}

func (job *PeriodicJob) AtMinuteInHour(minute, second int) (*PeriodicJob, error) {
	if err := job.cron.AtMinuteInHour(minute, second); err != nil {
		return nil, err
	}
	return job, nil
}

func (job *PeriodicJob) AtSecondInMinute(second int) (*PeriodicJob, error) {
	if err := job.cron.AtSecondInMinute(second); err != nil {
		return nil, err
	}
	return job, nil
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

var ErrNotFunctionType = errors.New("job's function is not function type")
var ErrFunctionArityNotMatch = errors.New("job's function arity does not match given parameters")

func callJobFuncWithParams(function interface{}, params []interface{}) ([]reflect.Value, error) {
	if reflect.TypeOf(function).Kind() != reflect.Func {
		return nil, ErrNotFunctionType
	}
	f := reflect.ValueOf(function)
	if len(params) != f.Type().NumIn() {
		return nil, ErrFunctionArityNotMatch
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	return f.Call(in), nil
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

func (cron *Cron) AtHourInDay(hour, minute, second int) error {
	if !isValidHour(hour) || !isValidMinute(minute) && !isValidSecond(second) {
		return ErrTimeRange
	}
	cron.at = time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute + time.Duration(second)*time.Second
	return nil
}

func (cron *Cron) AtMinuteInHour(minute, second int) error {
	if !isValidMinute(minute) || !isValidSecond(second) {
		return ErrTimeRange
	}
	cron.at = time.Duration(minute)*time.Minute + time.Duration(second)*time.Second
	return nil
}

func (cron *Cron) AtSecondInMinute(second int) error {
	if !isValidSecond(second) {
		return ErrTimeRange
	}
	cron.at = time.Duration(second) * time.Second
	return nil
}

func (cron *Cron) IntervalDuration() time.Duration {
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

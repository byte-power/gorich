package task

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

type Scheduler struct {
	jobs        map[string]ScheduledJob
	jobCount    int
	workerCount int
	queue       chan ScheduledJob
}

func (scheduler *Scheduler) AddPeriodicJob(name string, cron *Cron, jobFunction interface{}, params ...interface{}) (ScheduledJob, error) {
	scheduledJob := ScheduledJob{
		name:        name,
		jobFunction: jobFunction,
		params:      params,
		cron:        cron,
	}
	scheduler.jobs[name] = scheduledJob
	return scheduledJob, nil
}

func (scheduler *Scheduler) AddRunOnceJob(name string, delay time.Duration, jobFunction interface{}, params ...interface{}) (ScheduledJob, error) {
	scheduledJob := ScheduledJob{
		name:        name,
		delay:       delay,
		jobFunction: jobFunction,
		params:      params,
	}
	scheduler.jobs[name] = scheduledJob
	return scheduledJob, nil
}

func (scheduler *Scheduler) getRunnableJobs() []ScheduledJob {
	runnableJobs := []ScheduledJob{}
	for _, job := range scheduler.jobs {
		if job.isReady(time.Now()) {
			runnableJobs = append(runnableJobs, job)
		}
	}
	return runnableJobs
}

func (scheduler *Scheduler) Start() {
	fmt.Println("start scheduler")
	go scheduler.startWorkers(scheduler.workerCount)
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			scheduler.QueueRunnableJobs()
		}
		fmt.Println("finish for in Start")
	}
}

func (scheduler *Scheduler) QueueRunnableJobs() {
	runnableJobs := scheduler.getRunnableJobs()
	for _, job := range runnableJobs {
		scheduler.queue <- job
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

type ScheduledJob struct {
	name          string
	jobFunction   interface{}
	params        []interface{}
	cron          *Cron
	scheduledType string
	delay         time.Duration
	nextRun       time.Time
	scheduledTime time.Time
}

func (job *ScheduledJob) isReady(t time.Time) bool {
	scheduledTime := job.scheduledTime
	var unit time.Duration
	var at time.Duration
	switch job.cron.intervalType {
	case intervalDay:
		unit = 24 * time.Hour
		at = time.Duration(t.Hour())*time.Hour +
			time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second
	case intervalHour:
		unit = time.Hour
		at = time.Duration(t.Minute())*time.Minute +
			time.Duration(t.Second())*time.Second
	case intervalMinute:
		unit = time.Minute
		at = time.Duration(t.Second()) * time.Second
	case intervalSecond:
		unit = time.Second
		at = 0 * time.Second
	}
	var isReady bool
	if scheduledTime.IsZero() {
		if job.cron.at == 0 {
			isReady = true
		}
		if at == job.cron.at {
			isReady = true
		}
		isReady = false
	} else {
		if scheduledTime.Add(time.Duration(job.cron.interval)*unit).Add(at) == t {
			isReady = true
		} else {
			isReady = false
		}
	}
	return isReady
}

func (job *ScheduledJob) Run() {
	callJobFuncWithParams(job.jobFunction, job.params)
}

func NewScheduler(queueSize int) Scheduler {
	return Scheduler{
		jobs:        make(map[string]ScheduledJob, 0),
		workerCount: 1,
		queue:       make(chan ScheduledJob, queueSize),
	}
}

type Queue interface{}

type Worker interface{}

// type Job struct {
// 	name        string
// 	jobFunction interface{}
// 	params      []interface{}
// }

// func NewJob(name string, jobFunction interface{}, params ...interface{}) (Job, error) {
// 	return Job{name: name, jobFunction: jobFunction, params: params}, nil
// }

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

func isValidHour(hour int) bool {
	return (hour > 0) && (hour < 24)
}

func isValidMinute(minute int) bool {
	return (minute > 0) && (minute < 60)
}

func isValidSecond(second int) bool {
	return (second > 0) && (second < 60)
}

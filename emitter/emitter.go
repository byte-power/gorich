package emitter

import (
	"errors"
	"sync"
)

var ErrNoListenerFound = errors.New("listener not found for the given event")

type ListenerFunc func() error

type Listener struct {
	once bool
	fn   ListenerFunc
}

type Emitter struct {
	listeners map[string]Listener
	mutex     *sync.Mutex
}

func NewEmitter() *Emitter {
	return &Emitter{
		listeners: make(map[string]Listener),
		mutex:     &sync.Mutex{},
	}
}

func (emitter *Emitter) AddListener(event string, f ListenerFunc) {
	emitter.mutex.Lock()
	defer emitter.mutex.Unlock()
	emitter.listeners[event] = Listener{fn: f}
}

func (emitter *Emitter) AddOnceListener(event string, f ListenerFunc) {
	emitter.mutex.Lock()
	defer emitter.mutex.Unlock()
	emitter.listeners[event] = Listener{fn: f, once: true}
}

func (emitter *Emitter) RemoveListener(event string) {
	emitter.mutex.Lock()
	defer emitter.mutex.Unlock()
	delete(emitter.listeners, event)
}

func (emitter *Emitter) EmitSync(event string) error {
	listener, ok := emitter.find(event)
	if !ok {
		return ErrNoListenerFound
	}
	if listener.once {
		emitter.RemoveListener(event)
	}
	return listener.fn()
}

func (emitter *Emitter) find(event string) (Listener, bool) {
	emitter.mutex.Lock()
	defer emitter.mutex.Unlock()
	listener, ok := emitter.listeners[event]
	return listener, ok
}

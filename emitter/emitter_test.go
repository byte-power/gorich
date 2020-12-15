package emitter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func eventListener1() error {
	return nil
}
func TestAddListener(t *testing.T) {
	emitter := NewEmitter()
	_, ok := emitter.listeners["event1"]
	assert.False(t, ok)
	emitter.AddListener("event1", eventListener1)
	_, ok = emitter.listeners["event1"]
	assert.True(t, ok)
}

func TestAddOnceListener(t *testing.T) {
	emitter := NewEmitter()
	_, ok := emitter.listeners["event1"]
	assert.False(t, ok)
	emitter.AddOnceListener("event1", eventListener1)
	listener, ok := emitter.listeners["event1"]
	assert.True(t, ok)
	assert.True(t, listener.once)
}

func TestRemoveListener(t *testing.T) {
	emitter := NewEmitter()
	event := "event1"
	_, ok := emitter.listeners[event]
	assert.False(t, ok)
	emitter.AddListener(event, eventListener1)
	_, ok = emitter.listeners[event]
	assert.True(t, ok)
	emitter.RemoveListener(event)
	_, ok = emitter.listeners[event]
	assert.False(t, ok)
}

func TestEmitSyncNoListenerFound(t *testing.T) {
	emitter := NewEmitter()
	event := "event1"
	err := emitter.EmitSync(event)
	assert.Equal(t, ErrNoListenerFound, err)
}

func TestEmitSyncOnceListener(t *testing.T) {
	emitter := NewEmitter()
	event := "event1"

	i := 0
	listener := func() error {
		i = i + 1
		return nil
	}
	emitter.AddOnceListener(event, listener)
	emitter.EmitSync(event)
	_, ok := emitter.listeners[event]
	assert.False(t, ok)
	assert.Equal(t, 1, i)
}

func TestEmitSyncListener(t *testing.T) {
	emitter := NewEmitter()
	event := "event1"
	i := 0
	listener := func() error {
		i = i + 1
		return nil
	}
	emitter.AddListener(event, listener)

	emitter.EmitSync(event)
	_, ok := emitter.listeners[event]
	assert.True(t, ok)
	assert.Equal(t, 1, i)

	emitter.EmitSync(event)
	_, ok = emitter.listeners[event]
	assert.True(t, ok)
	assert.Equal(t, 2, i)
}

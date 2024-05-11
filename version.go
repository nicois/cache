package cache

import (
	"bytes"
	"fmt"
)

type Version interface {
	Current() []byte
	NotifyOnChange(initial []byte, onChanged chan []byte) int
	CancelNotifyOnChange(i int)
}

func CreateNullVersion() Version {
	// This generates a Version which returns nil, resulting
	// in nothing being cached.
	nullReactive := func() ([]byte, error) {
		return nil, fmt.Errorf("null version selected")
	}
	return Reactive(nullReactive)
}

func WaitForCurrent(v Version) []byte {
	c := make(chan []byte)
	defer v.CancelNotifyOnChange(v.NotifyOnChange([]byte(""), c))
	return (<-c)
}

/*
This type is used when you don't really care about being

	notified on changes; it's sufficient to be able to get
	the current value on demand.
*/
type Reactive func() ([]byte, error)

// Use to provide a version which will not change
func Static(f func() ([]byte, error)) Reactive {
	// Executes a "reactive" function a single time
	// and always return that value
	staticResult, staticError := f()
	return Reactive(func() ([]byte, error) {
		return staticResult, staticError
	})
}

// Use when there is only one version
func Identity() Reactive {
	return Static(func() ([]byte, error) {
		return []byte("identity"), nil
	})
}

func (r Reactive) Current() []byte {
	result, err := r()
	if err != nil {
		return nil
	}
	return result
}

func (r Reactive) NotifyOnChange(initial []byte, onChanged chan []byte) int {
	current := r.Current()
	if !bytes.Equal(current, initial) {
		go func() {
			onChanged <- current
		}()
	}
	return -1
}

func (r Reactive) CancelNotifyOnChange(i int) {
}

type hybrid struct {
	current Reactive
	abort   Version
}

func (h hybrid) Current() []byte {
	return h.current.Current()
}

func (h hybrid) NotifyOnChange(initial []byte, onChanged chan []byte) int {
	// initial value will be from current, so discard it
	return h.abort.NotifyOnChange(h.abort.Current(), onChanged)
}

func (h hybrid) CancelNotifyOnChange(i int) {
	h.abort.CancelNotifyOnChange(i)
}

func CreateHybrid(current Reactive, abort Version) Version {
	return hybrid{current: current, abort: abort}
}

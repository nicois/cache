package cache

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type staticListeners[T comparable] struct {
	value T
}

func (l staticListeners[T]) HasCurrent() bool {
	return true
}

func (l staticListeners[T]) Current() T {
	return l.value
}

func (l staticListeners[T]) NotifyOnChange(onChanged chan<- T) int {
	return 0
}

func (l staticListeners[_]) CancelNotifyOnChange(i int) {
}

type Reactive[T comparable] func() (T, error)

type reactiveListeners[T comparable] struct {
	r        Reactive[T]
	e        error
	validity time.Duration
	value    T
	valueAt  time.Time
}

func CreateReactiveListener[T comparable](r Reactive[T], validity time.Duration) *reactiveListeners[T] {
	return &reactiveListeners[T]{r: r, validity: validity}
}

func (r *reactiveListeners[T]) refreshIfNecessary() {
	now := time.Now()
	if now.Sub(r.valueAt) > r.validity {
		r.value, r.e = r.r()
		r.valueAt = now
	}
}

// Reset will clear in the internal cache, meaning the reactive function
// will be called next time, regardless of the elapsed time since the
// previous call.
func (r *reactiveListeners[_]) Reset() {
	r.valueAt = time.Time{}
}

func (r *reactiveListeners[_]) HasCurrent() bool {
	r.refreshIfNecessary()
	return r.e == nil
}

func (r *reactiveListeners[T]) Current() T {
	r.refreshIfNecessary()
	return r.value
}

func (r *reactiveListeners[T]) NotifyOnChange(onChanged chan<- T) int {
	return 0
}

func (r *reactiveListeners[_]) CancelNotifyOnChange(i int) {
}

type listeners[T comparable] struct {
	currentIsSet      bool
	current           T
	subscriptionIndex int
	subscriptionMutex *sync.Mutex
	subscriptions     map[int]chan<- T
}

func (l *listeners[T]) HasCurrent() bool {
	return l.currentIsSet
}

func (l *listeners[T]) Current() T {
	return l.current
}

func (l *listeners[T]) NotifyOnChange(onChanged chan<- T) int {
	log.Debugf("setting up change notifier")
	l.subscriptionMutex.Lock()
	defer l.subscriptionMutex.Unlock()
	l.subscriptionIndex++
	l.subscriptions[l.subscriptionIndex] = onChanged
	if l.currentIsSet {
		select {
		case onChanged <- l.current:
		default:
			log.Debugf("version has changed to %v but receive channel is full.", l.current)
		}
	}
	log.Debugf("unsubscribe index is %v", l.subscriptionIndex)
	return l.subscriptionIndex
}

func (l *listeners[_]) CancelNotifyOnChange(i int) {
	log.Debugln("cancelling notification on change")
	l.subscriptionMutex.Lock()
	defer l.subscriptionMutex.Unlock()
	delete(l.subscriptions, i)
}

func CreateStaticListener[T comparable](value T) staticListeners[T] {
	return staticListeners[T]{value: value}
}

func CreateListener[T comparable](source <-chan T) *listeners[T] {
	/*
	   creates a service which listens to a channel, propagating out each new value
	   to each subscribed listener
	*/
	result := listeners[T]{subscriptions: make(map[int]chan<- T), subscriptionMutex: new(sync.Mutex)}
	go func() {
		for {
			newValue := <-source
			if newValue != result.current || !result.currentIsSet {
				result.subscriptionMutex.Lock()
				log.Debugf("Notifying %v listeners that %v is now %v", len(result.subscriptions), result.current, newValue)
				result.current = newValue
				result.currentIsSet = true
				for _, channel := range result.subscriptions {
					// Best-effort: if the channel is full, don't block
					// If a consumer might have a full channel and wants to verify the current value
					// they can always call Current().
					select {
					case channel <- newValue:
					default:
					}
				}
				result.subscriptionMutex.Unlock()
			}
		}
	}()
	return &result
}

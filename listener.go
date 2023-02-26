package cache

import (
	"bytes"
	"sync"

	log "github.com/sirupsen/logrus"
)

type listeners struct {
	current           []byte
	subscriptionIndex int
	subscriptionMutex *sync.Mutex
	subscriptions     map[int]chan []byte
}

func (l *listeners) Current() []byte {
	// returns the most recent value
	return l.current
}

func (l *listeners) NotifyOnChange(initial []byte, onChanged chan []byte) int {
	log.Debugf("setting up change notifier with %v as initial.", initial)
	l.subscriptionMutex.Lock()
	defer l.subscriptionMutex.Unlock()
	l.subscriptionIndex++
	l.subscriptions[l.subscriptionIndex] = onChanged
	if current := l.current; !bytes.Equal(current, initial) {
		go func() {
			onChanged <- current
		}()
	}
	log.Debugf("unsubscribe index is %v", l.subscriptionIndex)
	return l.subscriptionIndex
}

func (l *listeners) CancelNotifyOnChange(i int) {
	log.Debugln("cancelling notification on change")
	l.subscriptionMutex.Lock()
	defer l.subscriptionMutex.Unlock()
	close(l.subscriptions[i])
	delete(l.subscriptions, i)
}

func CreateListener(source chan []byte) *listeners {
	/*
	   creates a service which listens to a channel, propagating out each new value
	   to each subscribed listener
	*/
	result := listeners{subscriptions: make(map[int]chan []byte), subscriptionMutex: new(sync.Mutex)}
	go func() {
		for {
			newValue := <-source
			if !bytes.Equal(newValue, result.current) {
				result.subscriptionMutex.Lock()
				log.Debugf("Notifying %v listeners that %v is now %v", len(result.subscriptions), string(result.current), string(newValue))
				result.current = newValue
				for _, listener := range result.subscriptions {
					select {
					case listener <- newValue:
					default:
					}
				}
				result.subscriptionMutex.Unlock()
			}
		}
	}()
	return &result
}

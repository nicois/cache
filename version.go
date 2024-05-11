package cache

import (
	"context"
)

// Version is used to track the current value of something, receiving a push to
// nominated channel(s) whenever the value changes.
// The channel will also be pushed the current value, if there is one at the time
// of registration.
type Version[T comparable] interface {
	HasCurrent() bool
	Current() T // only valid if HasCurrent()
	NotifyOnChange(onChanged chan<- T) int

	// CancelNotifyOnChange deregisters the NotifyOnChange registration.
	CancelNotifyOnChange(i int)
}

// CreateNullVersion is used as a no-op, when
// no version should ever be returned
func CreateNullVersion() Version[string] {
	c := make(chan string)
	return CreateListener(c)
}

func WaitForCurrent[T comparable](ctx context.Context, v Version[T]) error {
	c := make(chan T, 1)
	defer v.CancelNotifyOnChange(v.NotifyOnChange(c))
	select {
	case <-c:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func WaitForValue[T comparable](ctx context.Context, v Version[T], value T) error {
	c := make(chan T, 1)
	defer v.CancelNotifyOnChange(v.NotifyOnChange(c))
	for {
		select {
		case found := <-c:
			if found == value {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

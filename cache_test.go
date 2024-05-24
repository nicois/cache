package cache

import (
	"context"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type testStructure struct {
	foo string
	bar int
	baz []byte
}

func TestListener(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	ctx := context.Background()
	// if this test isn't completed within a second something
	// is terribly wrong
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	c, err := CreateInDirectory[testStructure, string](ctx, "cache-test", t.TempDir())
	require.NoError(t, err, "failed to create the cache")
	hasher := sha256.New()
	called := false
	ok := func(ctx context.Context, stdout io.Writer, stderr io.Writer) (result testStructure, err error) {
		called = true
		stdout.Write([]byte("out"))
		stderr.Write([]byte("err"))
		return testStructure{foo: "hi", bar: 33, baz: []byte("um")}, nil
	}

	versionChannel := make(chan string, 1)
	versioner := CreateListener(versionChannel)

	// The current value on the versioner will not be set
	// initially, meaning that the wrapper will not be cached
	result, err := c.Cache(ctx, hasher, ok, versioner)
	require.True(t, called)
	require.Equal(t, testStructure{foo: "hi", bar: 33, baz: []byte("um")}, result)
	require.NoError(t, err)

	// reset the called flag, then verify it is set
	// when the cache is used a second time, as the versioner
	// still doesn't have a value
	called = false
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.False(t, versioner.HasCurrent())
	require.True(t, called)
	require.Equal(t, testStructure{foo: "hi", bar: 33, baz: []byte("um")}, result)
	require.NoError(t, err)

	// When a version is sent, the current value will
	// be set shortly thereafter (but not immediately).
	// If we wait for a current value, we will be
	// rewarded.
	called = false
	versionChannel <- "v1"
	require.NoError(t, WaitForCurrent(ctx, versioner))
	require.Equal(t, versioner.Current(), "v1")
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.NoError(t, err)
	require.Equal(t, testStructure{foo: "hi", bar: 33, baz: []byte("um")}, result)
	require.True(t, called)

	// If the version changes, this should cause the wrapped
	// function to be called
	called = false
	versionChannel <- "v2"
	// we still need to wait as there's no guarantee the
	// new version has been assimilated otherwise.
	require.NoError(t, WaitForValue(ctx, versioner, "v2"))
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.NoError(t, err)
	require.Equal(t, testStructure{foo: "hi", bar: 33, baz: []byte("um")}, result)
	require.True(t, called)

	// now we'll cancel the context which should prevent the
	// function from being successful
	err = c.Truncate()
	require.NoError(t, err)
	called = false
	ctx, cancelCtx := context.WithCancel(ctx)
	cancelCtx()
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.Error(t, err)
	require.Equal(t, testStructure{}, result)
	require.False(t, called)
}

func TestStaticListener(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	c, err := CreateInDirectory[[]byte, string](ctx, "cache-test", t.TempDir())
	require.NoError(t, err, "failed to create the cache")
	hasher := sha256.New()
	called := false
	ok := func(ctx context.Context, stdout io.Writer, stderr io.Writer) (result []byte, err error) {
		called = true
		stdout.Write([]byte("out"))
		stderr.Write([]byte("err"))
		return []byte("ok"), nil
	}

	// the first time the function is called, it's actually executed
	// (as indicated by the `called` flag being set)
	versioner := CreateStaticListener("foo")
	result, err := c.Cache(ctx, hasher, ok, versioner)
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.True(t, called)

	// the second time, we get the result from the cache
	// without the function being called
	called = false
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.False(t, called)

	// this purges all records from the cache
	err = c.Truncate()
	require.NoError(t, err)

	// so we should see the function called this time
	called = false
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.True(t, called)

	// now we'll cancel the context which should prevent the
	// function from being successful
	err = c.Truncate()
	require.NoError(t, err)
	called = false
	ctx, cancelCtx := context.WithCancel(ctx)
	cancelCtx()
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.Error(t, err)
	require.Equal(t, result, []byte(nil))
	require.False(t, called)
}

func TestReactiveListener(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	c, err := CreateInDirectory[[]byte, string](ctx, "cache-test", t.TempDir())
	require.NoError(t, err, "failed to create the cache")
	hasher := sha256.New()
	called := false
	ok := func(ctx context.Context, stdout io.Writer, stderr io.Writer) (result []byte, err error) {
		called = true
		stdout.Write([]byte("out"))
		stderr.Write([]byte("err"))
		return []byte("ok"), nil
	}

	reactiveResponse := "r1"
	// the first time the function is called, it's actually executed
	// (as indicated by the `called` flag being set)
	versioner := CreateReactiveListener[string]((func() (string, error) { return reactiveResponse, nil }), time.Second/10)
	result, err := c.Cache(ctx, hasher, ok, versioner)
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.True(t, called)

	// the second time, we get the result from the cache
	// without the function being called, as neither the hasher
	// nor the versioner has changed
	called = false
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.False(t, called)

	// but now the versioner has been changed.
	reactiveResponse = "r2"
	versioner.Reset()
	called = false
	result, err = c.Cache(ctx, hasher, ok, versioner)
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.True(t, called)
}

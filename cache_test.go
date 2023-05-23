package cache

import (
	"context"
	"crypto/sha256"
	"io"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	c, err := CreateInDirectory(context.Background(), "cache-test", t.TempDir())
	require.NoError(t, err, "failed to create the cache")
	hasher := sha256.New()
	called := false
	ok := func(stdout io.Writer, stderr io.Writer, abort chan []byte) (result []byte, err error) {
		called = true
		stdout.Write([]byte("out"))
		stderr.Write([]byte("err"))
		return []byte("ok"), nil
	}

	// the first time the function is called, it's actually executed
	// (as indicated by the `called` flag being set)
	result, err := c.Cache(hasher, ok, Identity())
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.True(t, called)

	// the second time, we get the result from the cache
	// without the function being called
	called = false
	result, err = c.Cache(hasher, ok, Identity())
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.False(t, called)

	// this purges all records from the cache
	err = c.Truncate()
	require.NoError(t, err)

	// so we should see the function called this time
	called = false
	result, err = c.Cache(hasher, ok, Identity())
	require.NoError(t, err)
	require.Equal(t, result, []byte("ok"))
	require.True(t, called)
}

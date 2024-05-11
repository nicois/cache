package cache

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/nicois/fastdb"
	"github.com/nicois/file"
	log "github.com/sirupsen/logrus"
)

type (
	// A CacheableFunction is provided with writers for any stdout/stderr they produce,
	// along with a context which may be cancelled.
	CacheableFunction func(ctx context.Context, stdout io.Writer, stderr io.Writer) (result []byte, err error)
)

type Cacher[T comparable] interface {
	Cache(ctx context.Context, hasher hash.Hash, wrapped CacheableFunction, versioner Version[T]) ([]byte, error)
	SetDefaultValidity(d time.Duration)
	Close()
}

type cacher[T comparable] struct {
	db              fastdb.FastDB
	defaultValidity time.Duration
}

func (c *cacher[T]) Invalidate(h hash.Hash, v Version[T]) error {
	if h == nil {
		return fmt.Errorf("No hasher was provided")
	}
	if !v.HasCurrent() {
		// nothing to invalidate, as there's no version
		return nil
	}
	version := v.Current()
	versionB, err := json.Marshal(version)
	if err != nil {
		return err
	}
	cacheKey := h.Sum(versionB)
	hexCacheKey := hex.EncodeToString(cacheKey)
	sql := "DELETE FROM cache WHERE key = ?"
	if _, err := c.db.Writer().Exec(sql, hexCacheKey); err != nil {
		return err
	}
	return nil
}

func (c *cacher[_]) Truncate() error {
	sql := "DELETE FROM cache"
	if _, err := c.db.Writer().Exec(sql); err != nil {
		return err
	}
	return nil
}

func (c *cacher[_]) SetDefaultValidity(d time.Duration) {
	c.defaultValidity = d
}

func (c *cacher[_]) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

func Create[T comparable](ctx context.Context, name string) (*cacher[T], error) {
	usr, err := user.Current()
	if err != nil {
		sentry.CaptureException(err)
		log.Fatal(err)
	}
	cacheDir := filepath.Join(usr.HomeDir, ".cache")
	return CreateInDirectory[T](ctx, name, cacheDir)
}

func CreateInDirectory[T comparable](ctx context.Context, name string, cacheDir string) (*cacher[T], error) {
	if !file.DirExists(cacheDir) {
		err := os.MkdirAll(cacheDir, 0700)
		if err != nil {
			return nil, err
		}
	}
	cacheFile := filepath.Join(cacheDir, fmt.Sprintf("%v.sqlite3", name))
	db, err := fastdb.Open(cacheFile)
	if err != nil {
		return nil, err
	}
	sql := "CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY, value BLOB, stdout BLOB, stderr BLOB, expires INTEGER) STRICT;"
	if _, err := db.Writer().Exec(sql); err != nil {
		return nil, err
	}
	result := &cacher[T]{db: db, defaultValidity: time.Hour * 24}
	go result.cleanup(ctx)
	return result, nil
}

// Purge outdated records
func (c *cacher[_]) cleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	sql := "DELETE FROM cache WHERE expires <= ?"
	writer := c.db.Writer()

	if _, err := writer.Exec(sql, time.Now().Unix()); err != nil {
		// FIXME; log the error instead of panicing
		log.Fatal(err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := writer.Exec(sql, time.Now().Unix()); err != nil {
				// FIXME; log the error instead of panicing
				log.Fatal(err)
			}
		}
	}
}

func (c *cacher[T]) Cache(ctx context.Context, hasher hash.Hash, wrapped CacheableFunction, versioner Version[T]) ([]byte, error) {
	/*
			   hasher: provides a key for everything hashed by this
		       versioner:(optional) supplies volatile caching key, which may change during execution.
	*/
	if versioner == nil {
		return []byte(""), fmt.Errorf("Must supply a versioner")
	}
	if c.defaultValidity <= time.Millisecond {
		return []byte(""), fmt.Errorf("No default validity was configured")
	}
	if hasher == nil {
		return []byte(""), fmt.Errorf("No hasher was provided")
	}
	versionB := []byte("---")
	var version T
	var err error
	if versioner.HasCurrent() {
		version = versioner.Current()
		versionB, err = json.Marshal(version)
		if err != nil {
			return []byte(""), err
		}
	} else {
		versioner = nil
	}
	cacheKey := hasher.Sum(versionB)
	hexCacheKey := hex.EncodeToString(cacheKey)
	sql := "SELECT value, stdout, stderr FROM cache WHERE key = ? AND ? < expires"
	now := time.Now()
	rows, err := c.db.Reader().Query(sql, hexCacheKey, now.Unix())
	if err != nil {
		return []byte(""), err
	}
	log.Debugln("bar")
	defer rows.Close()
	for rows.Next() {
		var result []byte
		var stdout []byte
		var stderr []byte
		if err := rows.Scan(&result, &stdout, &stderr); err != nil {
			log.Debugln("could not parse the result")
			return []byte(""), err
		}
		if len(stdout) > 0 {
			os.Stdout.Write(stdout)
		}
		if len(stderr) > 0 {
			os.Stderr.Write(stderr)
		}
		log.Debugf("found a valid result: %q", result)
		//nolint:all // it is cleaner to handle the at-most-one-line case inside this scope
		return result, nil
	}
	log.Debugln("Found no valid results")
	select {
	case <-ctx.Done():
		log.Debugln("Context is cancelled, so not running the wrapped function")
		return []byte(""), ctx.Err()
	default:
	}

	var stdout, stderr bytes.Buffer
	log.Debugln("About to run the wrapped command")
	stdoutMw := io.MultiWriter(&stdout, os.Stdout)
	stderrMw := io.MultiWriter(&stderr, os.Stderr)
	onChange := make(chan T)
	if versioner != nil {
		go func() {
			// If the version changes, cancel the wrapped function (and self)
			ctx, cancelCtx := context.WithCancel(ctx)
			defer cancelCtx()
			defer versioner.CancelNotifyOnChange(versioner.NotifyOnChange(onChange))
			for {
				select {
				case <-onChange:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	result, resultError := wrapped(ctx, stdoutMw, stderrMw)
	sql = "INSERT INTO cache (key, value, stdout, stderr, expires) VALUES (?, ?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, stdout=excluded.stdout, stderr=excluded.stderr, expires=excluded.expires;"
	validUntil := now.Add(c.defaultValidity)
	if resultError == nil {
		if versioner == nil {
			log.Debugf("Not caching the result as no current version value is available.")
		} else {
			new_version := versioner.Current()
			if err != nil {
				log.Debugln("version is not available; not caching the result.")
				return result, nil
			}
			if new_version != version {
				log.Infoln("version has changed during execution; not caching the result.")
				return result, nil
			}
			if _, err := c.db.Writer().Exec(sql, hexCacheKey, result, stdout.Bytes(), stderr.Bytes(), validUntil.Unix()); err != nil {
				log.Error(err)
				return result, err
			}
			log.Debugf("Cached the result with key %v until %v", hexCacheKey, validUntil)
		}
	} else {
		log.Debugf("Not caching result as an error was returned: %v", resultError)
	}
	return result, resultError
}

package cache

import (
	"bytes"
	"context"
	"encoding/hex"
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
	CacheableFunction func(stdout io.Writer, stderr io.Writer, abort chan []byte) (result []byte, err error)
)

func (c *cacher) Invalidate(h hash.Hash, v Version) error {
	if h == nil {
		return fmt.Errorf("No hasher was provided")
	}
	version := v.Current()
	cacheKey := h.Sum(version)
	hexCacheKey := hex.EncodeToString(cacheKey)
	sql := "DELETE FROM cache WHERE key = ?"
	if _, err := c.db.Writer().Exec(sql, hexCacheKey); err != nil {
		return err
	}
	return nil
}

func (c *cacher) Truncate() error {
	sql := "DELETE FROM cache"
	if _, err := c.db.Writer().Exec(sql); err != nil {
		return err
	}
	return nil
}

type cacher struct {
	db              fastdb.FastDB
	defaultValidity time.Duration
}

func (c *cacher) SetDefaultValidity(d time.Duration) {
	c.defaultValidity = d
}

func (c *cacher) Close() {
	if c.db != nil {
		c.db.Close()
	}
}

type mockCacher struct{}

func (m *mockCacher) Cache(hasher hash.Hash, wrapped CacheableFunction, versioner Version) ([]byte, error) {
	// Same interface as the real cache, but doesn't ever cache
	return wrapped(os.Stdout, os.Stderr, make(chan []byte))
}

func (m *mockCacher) SetDefaultValidity(d time.Duration) {
}

func (m *mockCacher) Close() {
}

type Cacher interface {
	Cache(hasher hash.Hash, wrapped CacheableFunction, versioner Version) ([]byte, error)
	SetDefaultValidity(d time.Duration)
	Close()
}

func Create(ctx context.Context, name string) (*cacher, error) {
	usr, err := user.Current()
	if err != nil {
		sentry.CaptureException(err)
		log.Fatal(err)
	}
	cacheDir := filepath.Join(usr.HomeDir, ".cache")
	return CreateInDirectory(ctx, name, cacheDir)
}

func CreateInDirectory(ctx context.Context, name string, cacheDir string) (*cacher, error) {
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
	result := &cacher{db: db, defaultValidity: time.Hour * 24}
	go result.cleanup(ctx)
	return result, nil
}

// Purge outdated records
func (c *cacher) cleanup(ctx context.Context) {
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

func (c *cacher) Cache(hasher hash.Hash, wrapped CacheableFunction, versioner Version) ([]byte, error) {
	/*
			   hasher: provides a key for everything hashed by this
		       versioner:(optional) supplies volatile caching key, which may change during execution.
	*/
	if c.defaultValidity <= time.Millisecond {
		return []byte(""), fmt.Errorf("No default validity was configured")
	}
	if versioner == nil {
		versioner = Identity()
	}
	if hasher == nil {
		return []byte(""), fmt.Errorf("No hasher was provided")
	}
	version := versioner.Current()
	if version == nil {
		// can't cache this, just run it "normally"
		log.Debugln("version is not available; not caching.")
		// no override of stderr/stdout; let them go to the normal place
		log.Debugln("About to run the wrapped command")
		return wrapped(os.Stdout, os.Stderr, nil)
	}
	cacheKey := hasher.Sum(version)
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
		return result, nil
	}
	log.Debugln("Found no valid results")

	var stdout, stderr bytes.Buffer
	log.Debugln("About to run the wrapped command")
	stdoutMw := io.MultiWriter(&stdout, os.Stdout)
	stderrMw := io.MultiWriter(&stderr, os.Stderr)
	onChanged := make(chan []byte)
	defer versioner.CancelNotifyOnChange(versioner.NotifyOnChange(version, onChanged))
	result, resultError := wrapped(stdoutMw, stderrMw, onChanged)
	sql = "INSERT INTO cache (key, value, stdout, stderr, expires) VALUES (?, ?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, stdout=excluded.stdout, stderr=excluded.stderr, expires=excluded.expires;"
	validUntil := now.Add(c.defaultValidity)
	if resultError == nil {
		new_version := versioner.Current()
		if new_version == nil {
			log.Debugln("version is no longer available; not caching the result.")
			return result, nil
		}
		if !bytes.Equal(new_version, version) {
			log.Infoln("version has changed during execution; not caching the result.")
			return result, nil
		}
		if _, err := c.db.Writer().Exec(sql, hexCacheKey, result, stdout.Bytes(), stderr.Bytes(), validUntil.Unix()); err != nil {
			log.Error(err)
			return result, err
		}
		log.Debugf("Cached the result with key %v until %v", hexCacheKey, validUntil)
	} else {
		log.Debugf("Not caching result as an error was returned: %v", resultError)
	}
	return result, resultError
}

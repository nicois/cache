package cache

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"time"

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
	cacheFile := filepath.Join(c.cacheDir, "/.cache-"+hex.EncodeToString(cacheKey))
	if err := os.Remove(cacheFile); err == nil {
		return err
	}
	return nil
}

type cacher struct {
	cacheDir        string
	maximumDuration time.Duration
}

func (c *cacher) SetMaximumDuration(d time.Duration) {
	c.maximumDuration = d
}

type mockCacher struct{}

func (m *mockCacher) Cache(hasher hash.Hash, wrapped CacheableFunction, versioner Version) ([]byte, error) {
	// Same interface as the real cache, but doesn't ever cache
	return wrapped(os.Stdout, os.Stderr, make(chan []byte))
}

func (m *mockCacher) SetMaximumDuration(d time.Duration) {
}

type Cacher interface {
	Cache(hasher hash.Hash, wrapped CacheableFunction, versioner Version) ([]byte, error)
	SetMaximumDuration(d time.Duration)
}

func Create(name string) Cacher {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	cacheDir := filepath.Join(usr.HomeDir, ".cache", name)
	if !file.DirExists(cacheDir) {
		err := os.MkdirAll(cacheDir, 0700)
		if err != nil {
			log.Errorf("%v cannot be used as a cache directory, so caching is disabled.", cacheDir)
			return &mockCacher{}
		}
	}
	result := &cacher{cacheDir: cacheDir, maximumDuration: time.Hour * 24}
	go result.Cleanup()
	return result
}

func (c *cacher) Cleanup() {
	// delete cache files over a week old
	now := time.Now()
	filepath.WalkDir(c.cacheDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Debugf("Not listening for changes in %v: not readable", path)
			return nil
		}
		shouldRemove := false
		if d.IsDir() {
			if path != c.cacheDir {
				// Do not remove a path unless it's empty and an hour old, to avoid
				// situations where a file is being written there shortly.
				isEmpty, err := file.DirIsEmpty(path)
				if err != nil {
					log.Infof("While trying to work out if %v is empty: %v", path, err)
				}
				if fileInfo, err := os.Stat(path); err == nil {
					shouldRemove = fileInfo.ModTime().Add(time.Hour).Before(now)
				}
				if isEmpty && shouldRemove {
					log.Debugf("%v is empty and over an hour old, so will remove it.", path)
				}
			}
		} else {
			if fileInfo, err := os.Stat(path); err == nil {
				shouldRemove = fileInfo.ModTime().Add(time.Hour * 168).Before(now)

				if shouldRemove {
					log.Debugf("%v is over a week old, so deleting it.", path)
				}
			}
		}
		if shouldRemove {
			err = os.Remove(path)
			if err != nil {
				log.Debugf("While trying to delete %v: %v", path, err)
			}
		}
		return nil
	})
}

func GetMtime(path string) (time.Time, error) {
	fileinfo, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return fileinfo.ModTime(), nil
}

func (c *cacher) Cache(hasher hash.Hash, wrapped CacheableFunction, versioner Version) ([]byte, error) {
	/*
			   hasher: provides a key for everything hashed by this
		       versioner:(optional) supplies volatile caching key, which may change during execution.
	*/
	if c.maximumDuration <= time.Millisecond {
		return []byte(""), fmt.Errorf("No maximum duration was configured")
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
	// use a 2-layer deep caching structure, with 4096 entries maximum
	// at the top layer.
	intermediateHexSize := 4
	subDir := filepath.Join(c.cacheDir, hexCacheKey[:intermediateHexSize])
	cacheFile := filepath.Join(subDir, hexCacheKey[intermediateHexSize:]+"-cache")
	stdoutFile := filepath.Join(subDir, hexCacheKey[intermediateHexSize:]+"-stdout")
	stderrFile := filepath.Join(subDir, hexCacheKey[intermediateHexSize:]+"-stderr")
	result, err := file.ReadBytes(cacheFile)
	if err == nil {
		// Is this file too old? If so, remove it (and stderr/stdout) and ignore
		if mtime, err := GetMtime(cacheFile); err != nil || time.Now().Sub(mtime) > c.maximumDuration {
			if err != nil {
				log.Debugf("Removing cached %v as error: %v", cacheFile, err)
			} else {
				log.Debugf("Removing cached %v as it's too old: %v", cacheFile, time.Now().Sub(mtime))
			}
			os.Remove(cacheFile)
			os.Remove(stdoutFile)
			os.Remove(stderrFile)
		}

		stdout, err := file.ReadBytes(stdoutFile)
		// stderr/stdout files may not exist. This probably means
		// nothing was produced, so silently continue with the result.
		if err == nil {
			stderr, err := file.ReadBytes(stderrFile)
			if err == nil {
				os.Stdout.Write(stdout)
				os.Stderr.Write(stderr)
			}
		}
		// log.Debug("Using cached result.")
		return result, nil
	} else {
		os.MkdirAll(subDir, 0o700)
	}
	// couldn't read cached data for one reason or another
	log.Debugln(err)

	var stdout, stderr bytes.Buffer
	log.Debugln("About to run the wrapped command")
	stdoutMw := io.MultiWriter(&stdout, os.Stdout)
	stderrMw := io.MultiWriter(&stderr, os.Stderr)
	onChanged := make(chan []byte)
	defer versioner.CancelNotifyOnChange(versioner.NotifyOnChange(version, onChanged))
	result, resultError := wrapped(stdoutMw, stderrMw, onChanged)
	if resultError == nil {
		new_version := versioner.Current()
		if new_version == nil {
			log.Debugln("version is no longer available; not caching the result.")
			return result, err
		}
		if !bytes.Equal(new_version, version) {
			log.Infoln("version has changed during execution; not caching the result.")
			return result, err
		}
		file.Sem.Acquire(context.Background(), 3)
		defer file.Sem.Release(3)
		if handle, err := os.Create(cacheFile); err == nil {
			if _, err := handle.Write(result); err == nil {
				defer handle.Close()
				stdoutBytes := stdout.Bytes()
				stderrBytes := stderr.Bytes()
				if len(stdoutBytes) > 0 || len(stderrBytes) > 0 {
					if handle, err := os.Create(stdoutFile); err == nil {
						defer handle.Close()
						if _, err := handle.Write(stdout.Bytes()); err == nil {
							if handle, err := os.Create(stderrFile); err == nil {
								defer handle.Close()
								if _, err := handle.Write(stderr.Bytes()); err == nil {
								}
							}
						}
					}
				}
			} else {
				handle.Close()
				os.Remove(cacheFile)
				log.Debugf("Could not write cache: %v")
				return []byte{}, err
			}
		}
	} else {
		log.Debugf("Not caching result as an error was returned: %v\n", resultError)
	}
	return result, resultError
}

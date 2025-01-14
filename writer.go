package db

import (
	"database/sql"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eidng8/go-utils"
)

// CachedWriter is an interface for writing data to a database in a batched
// manner. This allows for better performance when writing a large number of
// records to the database, or write operation is too frequent.
type CachedWriter interface {
	// Push adds a record to the cache
	Push(any)
	// Write sends all cached data to the DB in a multi-value insert
	Write()
	// Start begins the writer and run until the given channel is signaled.
	// The writer will attempt to write data to the DB at the interval set by
	// SetInterval.
	Start(<-chan struct{})
	// Pause temporarily stops the writer from writing to the DB. Data can still
	// be added to the cache while the writer is paused.
	Pause()
	// Resume restarts the writer after a pause.
	Resume()
	// SetDB sets the database connection for the writer.
	SetDB(*sql.DB)
	// SetRetries sets the number of times the writer will attempt to write data
	// to the DB before giving up. Defaults to 3.
	SetRetries(int)
	// SetInterval sets the interval (second) at which the writer will attempt
	// to write data to the DB. Defaults to 1 second.
	SetInterval(time.Duration)
}

// DelegateCachedWriter is an interface for a CachedWriter that allows the user
// to set the query builder function that will be used to build the query and
// arguments for writing data to the DB.
type DelegateCachedWriter interface {
	// SetQueryBuilder sets the function that will be used to build the query
	// and arguments for writing data to the DB.
	SetQueryBuilder(func([]any) (string, []any))
}

type LoggedWriter interface {
	SetLogger(log utils.TaggedLogger)
}

func NewMemCachedWriter(
	db *sql.DB, sqlBuilder func(params []any) (string, []any),
) *MemCachedWriter {
	return &MemCachedWriter{
		db:         db,
		maxRetries: 3,
		interval:   1,
		builder:    sqlBuilder,
	}
}

// MemCachedWriter is a DelegateCachedWriter that writes string data to a
// database in a batched manner. It uses a query builder function to build the
// query and arguments for writing data to the DB.
type MemCachedWriter struct {
	db         *sql.DB
	dataCache  []any
	cacheMu    sync.Mutex
	maxRetries int
	interval   time.Duration
	failedLog  io.Writer
	paused     int32
	logger     utils.TaggedLogger
	builder    func([]any) (string, []any)
}

func (w *MemCachedWriter) SetLogger(log utils.TaggedLogger) {
	w.logger = log
}

func (w *MemCachedWriter) SetQueryBuilder(
	fn func([]any) (string, []any),
) {
	w.builder = fn
}

func (w *MemCachedWriter) SetDB(db *sql.DB) {
	w.db = db
}

func (w *MemCachedWriter) SetRetries(numRetries int) {
	w.maxRetries = numRetries
}

func (w *MemCachedWriter) SetInterval(duration time.Duration) {
	w.interval = duration
}

func (w *MemCachedWriter) SetFailedLog(log io.Writer) {
	w.failedLog = log
}

func (w *MemCachedWriter) Pause() {
	w.cacheMu.Lock()
	defer w.cacheMu.Unlock()
	atomic.StoreInt32(&w.paused, 1)
}

func (w *MemCachedWriter) Resume() {
	w.cacheMu.Lock()
	defer w.cacheMu.Unlock()
	atomic.StoreInt32(&w.paused, 0)
}

func (w *MemCachedWriter) Push(data any) {
	w.cacheMu.Lock()
	defer w.cacheMu.Unlock()
	w.dataCache = append(w.dataCache, data)
}

func (w *MemCachedWriter) Write() {
	if 1 == atomic.LoadInt32(&w.paused) {
		return
	}
	w.cacheMu.Lock()
	// holds the connection for the duration of the write operation
	// just in case it is paused or closed while the operation is in progress
	conn := w.db
	cached := w.dataCache
	count := len(cached)
	if count > 0 {
		w.dataCache = w.dataCache[:0]
	}
	w.cacheMu.Unlock()
	if 0 == count {
		return
	}
	var todo []any
	for i := 0; i < w.maxRetries; i++ {
		todo, cached = cached, nil
		for data := range slices.Chunk(todo, 1000) {
			query, args := w.builder(data)
			ok, _ := Transaction(
				conn,
				func(tx *sql.Tx) (bool, error) {
					_, err := tx.Exec(query, args...)
					return nil == err, err
				},
			)
			if !ok {
				cached = append(cached, args...)
			}
		}
		if len(cached) < 1 {
			break
		}
	}
	if len(cached) > 0 {
		w.logFailed(cached)
	}
}

func (w *MemCachedWriter) Start(stopChan <-chan struct{}) {
	ticker := time.NewTicker(w.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.Write()
			case <-stopChan:
				// final flush upon shutdown
				w.Write()
				return
			}
		}
	}()
}

func (w *MemCachedWriter) logFailed(failed []any) {
	if nil == w.failedLog || len(failed) < 1 {
		return
	}
	data, _ := utils.SliceMapFunc[[]string](failed, utils.MapToType)
	_, err := w.failedLog.Write([]byte(strings.Join(data, "\n")))
	if nil != err {
		w.logger.Errorf("Error writing failed data log: %v\n", err)
	}
}

package db

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_SetQueryBuilder(t *testing.T) {
	writer := MemCachedWriter{}
	writer.SetQueryBuilder(
		func(params []any) (string, []any) { return "test sql", params },
	)
	query, args := writer.builder([]any{"test", "arg"})
	require.Equal(t, "test sql", query)
	require.Equal(t, []any{"test", "arg"}, args)
}

func Test_SetLogger(t *testing.T) {
	logger := NewMockTaggedLogger(t)
	logger.EXPECT().Debugf("test %v", "arg").Once()
	writer := MemCachedWriter{}
	writer.SetLogger(logger)
	writer.logger.Debugf("test %v", "arg")
}

func Test_SetDB(t *testing.T) {
	db := setup(t)
	writer := MemCachedWriter{}
	writer.SetDB(db)
	require.Equal(t, db, writer.db)
}

func Test_SetRetries(t *testing.T) {
	writer := MemCachedWriter{}
	writer.SetRetries(5)
	require.Equal(t, 5, writer.maxRetries)
}

func Test_SetInterval(t *testing.T) {
	writer := MemCachedWriter{}
	writer.SetInterval(5)
	require.Equal(t, time.Duration(5), writer.interval)
}

func Test_SetFailedLog(t *testing.T) {
	writer := MemCachedWriter{}
	log := io.PipeWriter{}
	writer.SetFailedLog(&log)
	require.Same(t, &log, writer.failedLog)
}

func Test_Pause_Resume(t *testing.T) {
	writer := MemCachedWriter{}
	writer.Pause()
	require.Equal(t, int32(1), atomic.LoadInt32(&writer.paused))
	writer.Resume()
	require.Equal(t, int32(0), atomic.LoadInt32(&writer.paused))
}

func Test_Push(t *testing.T) {
	writer := MemCachedWriter{}
	writer.Push("test")
	require.Equal(t, []any{"test"}, writer.dataCache)
}

func Test_Write_on_timer(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	writer := NewMemCachedWriter(setup(t), nil)
	writer.Push("http://localhost/up?a=1&b=2")
	writer.SetQueryBuilder(
		func(params []any) (string, []any) {
			require.Equal(t, []any{"http://localhost/up?a=1&b=2"}, params)
			wg.Done()
			return "insert into test values(?)", []any{"test value"}
		},
	)
	stopChan := make(chan struct{})
	writer.Start(stopChan)
	time.Sleep(1100 * time.Microsecond)
	stopChan <- struct{}{}
	wg.Wait()
	close(stopChan)
	require.Empty(t, writer.dataCache)
	var count int
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	require.Nil(t, writer.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count))
	require.Equal(t, 1, count)
}

func Test_Write_pops_cache(t *testing.T) {
	writer := NewMemCachedWriter(setup(t), bt(t))
	writer.Push("http://localhost/up?a=1&b=2")
	writer.Write()
	require.Empty(t, writer.dataCache)
	var count int
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	require.Nil(t, writer.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count))
	require.Equal(t, 1, count)
}

func Test_Write_before_shutdown(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	writer := NewMemCachedWriter(setup(t), nil)
	writer.Push("http://localhost/up?a=1&b=2")
	writer.SetQueryBuilder(
		func(params []any) (string, []any) {
			require.Equal(t, []any{"http://localhost/up?a=1&b=2"}, params)
			wg.Done()
			return "insert into test values(?)", []any{"test value"}
		},
	)
	stopChan := make(chan struct{})
	writer.Start(stopChan)
	stopChan <- struct{}{}
	wg.Wait()
	close(stopChan)
	time.Sleep(100 * time.Microsecond)
	require.Empty(t, writer.dataCache)
	var count int
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	require.Nil(t, writer.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count))
	require.Equal(t, 1, count)
}

func Test_Write_is_paused(t *testing.T) {
	writer := NewMemCachedWriter(setup(t), bt(t))
	writer.Push("http://localhost/up?a=1&b=2")
	writer.Pause()
	writer.Write()
	require.Len(t, writer.dataCache, 1)
	var count int
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	require.Nil(t, writer.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count))
	require.Equal(t, 0, count)
	writer.Resume()
	writer.Write()
	require.Empty(t, writer.dataCache)
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	require.Nil(t, writer.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count))
	require.Equal(t, 1, count)
}

func Test_Write_does_nothing_if_cache_empty(t *testing.T) {
	writer := NewMemCachedWriter(setup(t), nil)
	writer.SetQueryBuilder(
		func(params []any) (string, []any) {
			require.FailNow(t, "should not be called")
			return "insert into test values(?)", []any{"test value"}
		},
	)
	writer.Write()
}

func Test_Write_retries_and_log_error(t *testing.T) {
	// logger := NewMockTaggedLogger(t)
	writer := NewMemCachedWriter(setup(t), nil)
	// writer.SetLogger(logger)
	writer.Push("val 0")
	times := 0
	writer.SetQueryBuilder(
		func(params []any) (string, []any) {
			require.Equal(t, []any{fmt.Sprintf("val %d", times)}, params)
			times++
			return "not a query", []any{fmt.Sprintf("val %d", times)}
		},
	)
	writer.Write()
	require.Equal(t, 3, times)
	// logger.EXPECT().Errorf(mock.Anything, mock.Anything).Once()
}

func Test_Write_retries_and_log_io_error(t *testing.T) {
	err := assert.AnError
	iow := NewMockWriter(t)
	logger := NewMockTaggedLogger(t)
	writer := NewMemCachedWriter(setup(t), nil)
	writer.SetRetries(1)
	writer.SetLogger(logger)
	writer.SetFailedLog(iow)
	iow.EXPECT().Write([]byte("test\nval")).Return(0, err).Once()
	logger.EXPECT().Errorf(mock.Anything, err).Once()
	writer.logFailed([]any{"test", "val"})
}

func Test_Write_concurrency(t *testing.T) {
	threads := 10
	var wg sync.WaitGroup
	wg.Add(threads * 2)
	writer := NewMemCachedWriter(setup(t), nil)
	writer.SetQueryBuilder(
		func(params []any) (string, []any) {
			pl := len(params)
			qs := strings.Repeat(",(?)", pl)[1:]
			as := make([]any, pl)
			for i := 0; i < pl; i++ {
				as[i] = "test value"
			}
			return fmt.Sprintf("insert into test values %s", qs), as
		},
	)
	for i := 0; i < threads; i++ {
		go func() {
			writer.Push("http://localhost/up?a=1&b=2")
			wg.Done()
		}()
		go func() {
			writer.Write()
			wg.Done()
		}()
	}
	stopChan := make(chan struct{})
	writer.Start(stopChan)
	// timer := timeout(t, 5*time.Second)
	time.Sleep(1100 * time.Microsecond)
	wg.Wait()
	// timer <- true
	close(stopChan)
	time.Sleep(100 * time.Microsecond)
	require.Empty(t, writer.dataCache)
	var count int
	//goland:noinspection SqlNoDataSourceInspection,SqlResolve
	require.Nil(t, writer.db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count))
	require.Equal(t, 10, count)
}

func bt(t *testing.T) SqlBuilderFunc {
	t.Helper()
	return func(params []any) (string, []any) {
		require.Equal(t, []any{"http://localhost/up?a=1&b=2"}, params)
		return "insert into test values(?)", []any{"test value"}
	}
}

func timeout(t *testing.T, d time.Duration) chan bool {
	t.Helper()
	done := make(chan bool)
	select {
	case <-time.After(d):
		require.Fail(t, "The test took too long.")
	case <-done:
	}
	return done
}

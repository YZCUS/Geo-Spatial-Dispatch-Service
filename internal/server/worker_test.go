package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_BasicExecution(t *testing.T) {
	processor := func(ctx context.Context, job Job) Result {
		// Simulate work
		return Result{
			JobID:  job.ID,
			Output: job.Payload.(int) * 2,
		}
	}

	pool := NewWorkerPool(3, 10, processor)
	pool.Start()
	defer pool.Stop()

	// Submit jobs
	for i := 0; i < 5; i++ {
		pool.Submit(Job{ID: string(rune('A' + i)), Payload: i + 1})
	}

	// Collect results
	results := make(map[string]int)
	for i := 0; i < 5; i++ {
		result := <-pool.Results()
		results[result.JobID] = result.Output.(int)
	}

	// Verify results
	expected := map[string]int{"A": 2, "B": 4, "C": 6, "D": 8, "E": 10}
	for id, val := range expected {
		if results[id] != val {
			t.Errorf("Job %s: expected %d, got %d", id, val, results[id])
		}
	}
}

func TestWorkerPool_ConcurrentProcessing(t *testing.T) {
	var processed int64

	processor := func(ctx context.Context, job Job) Result {
		atomic.AddInt64(&processed, 1)
		time.Sleep(10 * time.Millisecond)
		return Result{JobID: job.ID}
	}

	pool := NewWorkerPool(5, 100, processor)
	pool.Start()

	start := time.Now()

	// Submit 20 jobs
	for i := 0; i < 20; i++ {
		pool.Submit(Job{ID: string(rune(i))})
	}

	// Collect all results
	for i := 0; i < 20; i++ {
		<-pool.Results()
	}

	pool.Stop()
	elapsed := time.Since(start)

	// With 5 workers and 10ms per job, 20 jobs should take ~40ms (not 200ms)
	if elapsed > 100*time.Millisecond {
		t.Errorf("Expected parallel execution, took %v", elapsed)
	}

	if atomic.LoadInt64(&processed) != 20 {
		t.Errorf("Expected 20 processed, got %d", processed)
	}
}

func TestWorkerPool_ErrorHandling(t *testing.T) {
	expectedErr := errors.New("processing failed")

	processor := func(ctx context.Context, job Job) Result {
		if job.Payload.(bool) {
			return Result{JobID: job.ID, Error: expectedErr}
		}
		return Result{JobID: job.ID, Output: "success"}
	}

	pool := NewWorkerPool(2, 10, processor)
	pool.Start()
	defer pool.Stop()

	pool.Submit(Job{ID: "fail", Payload: true})
	pool.Submit(Job{ID: "success", Payload: false})

	results := make(map[string]Result)
	for i := 0; i < 2; i++ {
		r := <-pool.Results()
		results[r.JobID] = r
	}

	if results["fail"].Error != expectedErr {
		t.Error("Expected error for fail job")
	}
	if results["success"].Error != nil {
		t.Error("Expected no error for success job")
	}
}

func TestWorkerPool_SubmitAndWait(t *testing.T) {
	processor := func(ctx context.Context, job Job) Result {
		return Result{
			JobID:  job.ID,
			Output: job.Payload.(int) + 10,
		}
	}

	pool := NewWorkerPool(3, 10, processor)
	pool.Start()
	defer pool.Stop()

	jobs := []Job{
		{ID: "1", Payload: 1},
		{ID: "2", Payload: 2},
		{ID: "3", Payload: 3},
	}

	results := pool.SubmitAndWait(jobs)

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	sum := 0
	for _, r := range results {
		sum += r.Output.(int)
	}

	// (1+10) + (2+10) + (3+10) = 36
	if sum != 36 {
		t.Errorf("Expected sum 36, got %d", sum)
	}
}

func TestWorkerPool_Shutdown(t *testing.T) {
	processor := func(ctx context.Context, job Job) Result {
		select {
		case <-time.After(1 * time.Second):
			return Result{JobID: job.ID}
		case <-ctx.Done():
			return Result{JobID: job.ID, Error: ctx.Err()}
		}
	}

	pool := NewWorkerPool(2, 10, processor)
	pool.Start()

	pool.Submit(Job{ID: "long-running"})

	// Shutdown should cancel workers immediately
	start := time.Now()
	pool.Shutdown()
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("Shutdown took too long: %v", elapsed)
	}
}

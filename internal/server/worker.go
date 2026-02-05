package server

import (
	"context"
	"log"
	"sync"
)

// Job represents a unit of work to be processed
type Job struct {
	ID      string
	Payload interface{}
}

// Result represents the outcome of processing a Job
type Result struct {
	JobID  string
	Output interface{}
	Error  error
}

// WorkerPool manages a pool of workers for concurrent job processing
type WorkerPool struct {
	workerCount int
	jobs        chan Job
	results     chan Result
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	processor   JobProcessor
}

// JobProcessor defines how to process a job
type JobProcessor func(ctx context.Context, job Job) Result

// NewWorkerPool creates a new worker pool with the specified number of workers
func NewWorkerPool(workerCount int, bufferSize int, processor JobProcessor) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	log.Printf("[WorkerPool] Creating pool with %d workers, buffer size %d", workerCount, bufferSize)
	return &WorkerPool{
		workerCount: workerCount,
		jobs:        make(chan Job, bufferSize),
		results:     make(chan Result, bufferSize),
		ctx:         ctx,
		cancel:      cancel,
		processor:   processor,
	}
}

// Start launches all workers
func (wp *WorkerPool) Start() {
	log.Printf("[WorkerPool] Starting %d workers", wp.workerCount)
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker processes jobs from the jobs channel
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	log.Printf("[Worker %d] Started", id)

	for {
		select {
		case <-wp.ctx.Done():
			log.Printf("[Worker %d] Shutting down (context cancelled)", id)
			return
		case job, ok := <-wp.jobs:
			if !ok {
				log.Printf("[Worker %d] Shutting down (jobs channel closed)", id)
				return
			}
			log.Printf("[Worker %d] Processing job %s", id, job.ID)
			result := wp.processor(wp.ctx, job)
			if result.Error != nil {
				log.Printf("[Worker %d] Job %s failed: %v", id, job.ID, result.Error)
			} else {
				log.Printf("[Worker %d] Job %s completed", id, job.ID)
			}
			select {
			case wp.results <- result:
			case <-wp.ctx.Done():
				return
			}
		}
	}
}

// Submit adds a job to the pool
func (wp *WorkerPool) Submit(job Job) bool {
	select {
	case wp.jobs <- job:
		return true
	case <-wp.ctx.Done():
		return false
	}
}

// Results returns the results channel for reading
func (wp *WorkerPool) Results() <-chan Result {
	return wp.results
}

// Stop gracefully shuts down the worker pool
func (wp *WorkerPool) Stop() {
	log.Printf("[WorkerPool] Stopping gracefully")
	close(wp.jobs)
	wp.wg.Wait()
	close(wp.results)
	log.Printf("[WorkerPool] Stopped")
}

// Shutdown immediately cancels all workers
func (wp *WorkerPool) Shutdown() {
	log.Printf("[WorkerPool] Shutting down immediately")
	wp.cancel()
	wp.wg.Wait()
	log.Printf("[WorkerPool] Shutdown complete")
}

// SubmitAndWait submits multiple jobs and waits for all results
func (wp *WorkerPool) SubmitAndWait(jobs []Job) []Result {
	var resultsMu sync.Mutex
	results := make([]Result, 0, len(jobs))

	// Collect results in background
	done := make(chan struct{})
	go func() {
		for i := 0; i < len(jobs); i++ {
			result := <-wp.results
			resultsMu.Lock()
			results = append(results, result)
			resultsMu.Unlock()
		}
		close(done)
	}()

	// Submit all jobs
	for _, job := range jobs {
		wp.Submit(job)
	}

	// Wait for all results
	<-done
	return results
}

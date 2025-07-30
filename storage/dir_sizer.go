package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const (
	maxJobs         = 8   // max number of jobs for asynchronous run
	maxDirsLenQueue = 100 // max number elements of queue for processing dirs
)

var (
	errNoExistObjs = errors.New("don't get dirs and files")
	errNoExistFile = errors.New("file doesn't exist")
)

// Result represents the Size function result
type Result struct {
	// Total Size of File objects
	Size int64
	// Count is a count of File objects processed
	Count int64
}

type DirSizer interface {
	// Size calculate a size of given Dir, receive a ctx and the root Dir instance
	// will return Result or error if happened
	Size(ctx context.Context, d Dir) (Result, error)
}

// sizer implement the DirSizer interface
type sizer struct {
	// maxWorkersCount number of workers for asynchronous run
	maxWorkersCount int

	// TODO: add other fields as you wish
}

// iterSizer implement the DirSizer interface
type iterSizer struct {
	// maxWorkersCount number of workers for asynchronous run
	maxWorkersCount int

	// TODO: add other fields as you wish
}

// NewSizer returns new DirSizer instance
func NewSizer() DirSizer {
	maxWorkers := maxJobs
	if maxWorkers <= 0 {
		maxWorkers = 8
	}

	return &iterSizer{
		maxWorkersCount: maxWorkers,
	}
}

// Recursion method
func (s *sizer) Size(parctx context.Context, d Dir) (result Result, err error) {
	var totalSize, totalCount int64
	eg, ctx := errgroup.WithContext(parctx)
	// Limit numbers of workers
	eg.SetLimit(s.maxWorkersCount)

	// Function for recursion processing
	var processDir func(Dir) error
	processDir = func(dir Dir) error {
		// Get sub-directories and files
		subDirs, files, err := dir.Ls(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", errNoExistObjs, err)
		}

		// Processing current files
		for _, file := range files {
			eg.Go(func() error {
				// Get file size
				size, err := file.Stat(ctx)
				if err != nil {
					return fmt.Errorf("%w: %w", errNoExistFile, err)
				}

				// atomic sum for safe counting
				atomic.AddInt64(&totalSize, size)
				atomic.AddInt64(&totalCount, 1)

				return nil
			})
		}

		// Processing current sub-directories
		for _, subDir := range subDirs {
			// Copy for recursion
			eg.Go(func() error {
				// recursive call to process sub-directory
				return processDir(subDir)
			})
		}

		return nil
	}

	// start from parent directory
	if err := processDir(d); err != nil {
		return Result{}, err
	}

	// wait all goroutines
	if err := eg.Wait(); err != nil {
		return Result{}, err
	}

	return Result{totalSize, totalCount}, nil
}

// Iterative method
func (s *iterSizer) Size(parctx context.Context, d Dir) (result Result, err error) {
	var totalSize, totalCount int64
	var wg sync.WaitGroup
	numDirWorkers := s.maxWorkersCount - 5 // need to be less than s.maxWorkersCount -- other for file parallel processing
	eg, ctx := errgroup.WithContext(parctx)
	// Limit numbers of workers
	eg.SetLimit(s.maxWorkersCount)

	// Queue for directories
	dirsToProcess := make(chan Dir, maxDirsLenQueue)

	// Process directories in parallel by queue
	for range numDirWorkers {
		eg.Go(func() error {
			for {
				dir, ok := <-dirsToProcess
				// Check close channel
				if !ok {
					return nil
				}

				// Process current directory
				if err := processCurDirectiry(ctx, eg, dir, dirsToProcess, &wg, &totalSize, &totalCount); err != nil {
					return err
				}
			}
		})
	}

	// Start queue
	wg.Add(1)
	dirsToProcess <- d

	// Close channel
	go func() {
		wg.Wait()
		close(dirsToProcess)
	}()

	// Wait all goroutines
	if err := eg.Wait(); err != nil {
		return Result{}, err
	}

	return Result{totalSize, totalCount}, nil
}

func processCurDirectiry(
	ctx context.Context,
	eg *errgroup.Group,
	dir Dir,
	dirsToProcess chan<- Dir,
	wg *sync.WaitGroup,
	totalSize, totalCount *int64,
) error {
	// Decrease counter
	defer wg.Done()

	// Process current directory
	subDirs, files, err := dir.Ls(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", errNoExistObjs, err)
	}

	// Process current files
	for _, file := range files {
		eg.Go(func() error {
			size, err := file.Stat(ctx)
			if err != nil {
				return fmt.Errorf("%w: %w", errNoExistFile, err)
			}

			atomic.AddInt64(totalSize, size)
			atomic.AddInt64(totalCount, 1)

			return nil
		})
	}

	// Add subDirs to queue
	for _, subDir := range subDirs {
		wg.Add(1)
		dirsToProcess <- subDir
	}

	return nil
}

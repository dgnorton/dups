package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apoorvam/goterminal"
	"github.com/cespare/xxhash/v2"
)

var (
	// ErrEmptyFile is returned when an empty file is encountered.
	ErrEmptyFile error = errors.New("empty file")
)

// FileKey is the type used as a lookup key for individual files.
type FileKey [64]byte

// Hash is a hash of the full contents of a file.
type Hash uint64

// File represents a file on disk. This type is safe for concurrent use.
type File struct {
	mu   sync.RWMutex
	Path string
	Info os.FileInfo
	hash Hash
	key  []byte
}

// NewFile returns a new File.
func NewFile(path string) (*File, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		return nil, fmt.Errorf("%q is a directory, not a file", path)
	}

	file := &File{
		Path: path,
		Info: fi,
	}

	return file, nil
}

// Key returns an identifier key for the underlying file. This key is cheap
// to retrieve relative to a full hash but also has a significantly higher
// chance of not being unique.
func (f *File) Key() (FileKey, error) {
	var key FileKey

	// See if we have key cached.
	f.mu.RLock()
	if f.key != nil {
		copy(key[:], f.key)
		f.mu.RUnlock()
		return key, nil
	}
	f.mu.RUnlock()

	// Key isn't cached so read it from file.
	fh, err := os.Open(f.Path)
	if err != nil {
		return key, err
	}
	defer fh.Close()

	n, err := fh.Read(key[:])
	if err != nil && err != io.EOF {
		return key, err
	} else if n == 0 {
		return key, ErrEmptyFile
	}

	// Store the key in the cache.
	f.mu.Lock()
	f.key = key[:]
	f.mu.Unlock()

	return key, nil
}

// Returns a non-cryptographic hash of the entire contents of the file.
func (f *File) Hash() (Hash, error) {
	// See if the hash is in the cache.
	f.mu.RLock()
	if f.hash != 0 {
		hash := f.hash
		f.mu.RUnlock()
		return hash, nil
	}
	f.mu.RUnlock()

	// Hash isn't cached so hash the file.
	fh, err := os.Open(f.Path)
	if err != nil {
		return 0, err
	}
	defer fh.Close()

	digest := xxhash.New()
	_, err = io.Copy(digest, fh)
	if err != nil {
		return 0, err
	}
	hash := digest.Sum64()

	// Cache the hash.
	f.mu.Lock()
	f.hash = Hash(hash)
	f.mu.Unlock()

	return f.hash, nil
}

// Files represents a list of files. This type is safe for concurrent use.
type Files struct {
	mu    sync.RWMutex
	files []*File
}

// NewFiles returns a new instance of Files.
func NewFiles() *Files {
	return &Files{
		files: []*File{},
	}
}

// Add adds a new file to the list and returns the number of files in the list
// after the new file is added.
func (f *Files) Add(file *File) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = append(f.files, file)
	return len(f.files)
}

// Duplicates returns a map of hashes to files that all had the same hash value.
func (f *Files) Duplicates() (map[Hash][]*File, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if len(f.files) < 2 {
		return nil, nil
	}

	dups := map[Hash][]*File{}

	for _, file := range f.files {
		hash, err := file.Hash()
		if err != nil {
			return nil, err
		}
		files := dups[hash]
		if files == nil {
			files = []*File{file}
		} else {
			files = append(files, file)
		}
		dups[hash] = files
	}

	for hash, files := range dups {
		if len(files) < 2 {
			delete(dups, hash)
		}
	}

	return dups, nil
}

// Tracker tracks files and finds potentially duplicate files. This type
// is safe for concurrent use.
// Note that two files are considered to be duplicates of each other if
// they have the same hash value. It is possible for two or more files
// to have different contents but the same hash. A byte-for-byte comparrison
// would be required to determin if the files were in fact duplicates.
type Tracker struct {
	mu    sync.RWMutex
	files map[FileKey]*Files
}

// NewTracker returns a new Tracker.
func NewTracker() *Tracker {
	return &Tracker{
		files: make(map[FileKey]*Files, 10000),
	}
}

// Add adds a file to the tracker.
func (t *Tracker) Add(path string) error {
	file, err := NewFile(path)
	if err != nil {
		return err
	}

	key, err := file.Key()
	if err != nil {
		return err
	}

	// First, check under read lock to see if we already have files associated
	// with this key.
	t.mu.RLock()
	files := t.files[key]
	t.mu.RUnlock()

	if files == nil {
		t.mu.Lock()
		// No files were associated with the key when we checked under read lock
		// but we need to check again under write lock before adding an entry for
		// this key.
		files = t.files[key]
		if files == nil {
			files = NewFiles()
			t.files[key] = files
		}
		t.mu.Unlock()
	}

	if cnt := files.Add(file); cnt > 1 {
		file.Hash()
	}

	return nil
}

// Duplicates returns a map of hashes to files with that hash.
func (t *Tracker) Duplicates() (map[Hash][]*File, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Map full file hash (not key) to a list of potential duplicate files.
	dups := map[Hash][]*File{}

	for _, files := range t.files {
		ds, err := files.Duplicates()
		if err != nil {
			return nil, err
		}
		if ds == nil {
			continue
		}
		for hash, fs := range ds {
			m := dups[hash]
			if m == nil {
				m = []*File{}
			}
			m = append(m, fs...)
			dups[hash] = m
		}
	}

	return dups, nil
}

// processFiles reads directory paths from a channel and adds all the files in
// each directory to the given tracker. It does not recurse sub directories and
// it ignores all files that aren't normal files (e.g., symlinks, pipes, etc.)
// This function runs in one or more goroutines, functioning as background
// workers.
func processFiles(id int, dirs chan string, tracker *Tracker, progress Progressor, wg *sync.WaitGroup) error {
	defer wg.Done()
	exitmsg := fmt.Sprintf("worker %d exited", id)
	defer progress.Update(id, exitmsg)

	for dir := range dirs {
		progress.Update(id, dir)
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.Mode()&os.ModeType != 0 {
				continue
			}
			name := filepath.Join(dir, f.Name())
			if err := tracker.Add(name); err != nil && err != ErrEmptyFile {
				continue
			}
			progress.IncFileCnt()
		}
	}

	return nil
}

func main() {
	var (
		basedir     string
		concurrency int
		showprog    bool
		stats       bool
	)

	// Default concurrency to the number of CPUs.
	concurrency = runtime.NumCPU()
	if concurrency < 1 {
		concurrency = 1
	}

	// Parse command line arguments.
	flag.IntVar(&concurrency, "c", concurrency, "number background worker routines")
	flag.BoolVar(&showprog, "p", false, "show progress")
	flag.BoolVar(&stats, "s", false, "show stats")
	flag.Parse()

	args := flag.Args()

	// Make sure we have the right number of command line arguments.
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "error: expected 1 argument.\nusage: dups PATH\n")
		os.Exit(1)
	}

	// Make sure the input path is a directory.
	basedir = args[0]
	fi, err := os.Stat(basedir)
	check("stat base directory", err)

	if !fi.IsDir() {
		fmt.Fprintf(os.Stderr, "%q is not a directory", basedir)
		os.Exit(1)
	}

	// Create a progress writer, if requested.
	var progress Progressor
	if showprog {
		progress = NewProgress(concurrency)
	} else {
		progress = &NoProgress{}
	}

	// Start background workers to process files.
	tracker := NewTracker()
	dirs := make(chan string)
	stop := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(concurrency)

	for id := 0; id < concurrency; id++ {
		go processFiles(id, dirs, tracker, progress, wg)
	}

	// This will signal main thread that all workers stopped.
	go func() { wg.Wait(); close(stop) }()

	// Walk the base directory recursively, sending each sub directory path to
	// the background workers to process the files in it.
	err = filepath.Walk(basedir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			select {
			case dirs <- path:
			case <-stop:
				return errors.New("all workers stopped")
			}
		}
		return nil
	})
	check("walking directory", err)

	// Wait for background workers to finish.
	close(dirs)
	wg.Wait()

	progress.Clear()
	progress.Update(0, "finished quick scan, performing deeper scan of possible duplicates...")

	// Output the duplicates.
	dups, err := tracker.Duplicates()
	check("getting duplicates", err)

	progress.Clear()

	dupcnt := 0
	for hash, files := range dups {
		dupcnt += len(files) - 1
		fmt.Printf("hash: %d\n", hash)
		for _, f := range files {
			fmt.Printf("dup:  %s\n", f.Path)
		}
	}

	// Output stats if requested.
	if stats {
		fmt.Printf("total files: %d\nduplicates:  %d\n", progress.FileCnt(), dupcnt)
	}
}

// check checks if there is an error and outputs it to STDERR, then exits the app.
func check(desc string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", desc, err)
		os.Exit(1)
	}
}

// Progressor is progress output interface. Implementations allow background
// worker routines to update one line in the progress output at a specified
// line index.
type Progressor interface {
	// Update updates the progress output at the specified line index.
	Update(line int, msg string)
	// IncFileCount increments a counter of the total number of files seen.
	IncFileCnt()
	// FileCnt returns the total count of files seen.
	FileCnt() uint64
	// Clear clears the progress output.
	Clear()
}

// Progress provides a progress indicator in the terminal.
type Progress struct {
	cnt   uint64
	mu    sync.Mutex
	lines []string
	w     *goterminal.Writer
}

// NewProgress returns a new Progress, which is an implementation of the
// Progressor interface that writes to the terminal.
func NewProgress(lines int) *Progress {
	return &Progress{
		lines: make([]string, lines),
		w:     goterminal.New(os.Stdout),
	}
}

func (p *Progress) Update(line int, msg string) {
	p.mu.Lock()
	p.w.Clear()
	p.lines[line] = msg
	p.render()
	p.mu.Unlock()
}

func (p *Progress) IncFileCnt() {
	atomic.AddUint64(&p.cnt, 1)
}

func (p *Progress) FileCnt() uint64 {
	return atomic.LoadUint64(&p.cnt)
}

func (p *Progress) Clear() {
	p.mu.Lock()
	for i := range p.lines {
		p.lines[i] = ""
	}
	p.w.Clear()
	p.mu.Unlock()
}

func (p *Progress) render() {
	fmt.Fprintf(p.w, "total files: %d\n", atomic.LoadUint64(&p.cnt))
	for i := range p.lines {
		fmt.Fprintln(p.w, p.lines[i])
	}
	p.w.Print()
}

// NoProgress returns a new NoProgress, which is an implementation of the
// Progressor interface that does not write any output.
type NoProgress struct {
	cnt uint64
}

func (p *NoProgress) Update(line int, msg string) {}
func (p *NoProgress) IncFileCnt() {
	atomic.AddUint64(&p.cnt, 1)
}

func (p *NoProgress) FileCnt() uint64 {
	return atomic.LoadUint64(&p.cnt)
}
func (p *NoProgress) Clear() {}

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		listFile string
		dir      string
		threads  int
		interval int
	)
	flag.StringVar(&listFile, "file", "", "list file")
	flag.StringVar(&dir, "basedir", "", "base directory")
	flag.IntVar(&threads, "threads", runtime.NumCPU(), "threads count")
	flag.IntVar(&interval, "interval", 0, "process lag in ms")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	file, err := os.Open(listFile)
	if err != nil {
		panic(fmt.Sprintf("open file. path: %s  error: %v", listFile, err))
	}
	defer file.Close()
	c := readRequest(file)
	r := Reader{dir: dir}

	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go r.read(c, &wg)
	}

	wg.Wait()
}

type Reader struct {
	dir string

	readBytes     int64
	lastReadBytes int64
	readCount     int64
	lastReadCount int64
	interval      time.Duration

	once sync.Once
}

type Size float64

func (s Size) String() string {
	if s < 1024 {
		return fmt.Sprintf("%4.3f B/s", s)
	}
	s = s / 1024
	if s < 1024 {
		return fmt.Sprintf("%4.3fKB/s", s)
	}
	s = s / 1024
	if s < 1024 {
		return fmt.Sprintf("%4.3fMB/s", s)
	}
	s = s / 1024
	if s < 1024 {
		return fmt.Sprintf("%4.3fGB/s", s)
	}
	s = s / 1024
	if s < 1024 {
		return fmt.Sprintf("%4.3fTB/s", s)
	}
	s = s / 1024
	return fmt.Sprintf("%4.3fPB/s", s)
}

func (r *Reader) read(pathC <-chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	r.once.Do(func() {
		if r.dir != "" {
			if err := os.Chdir(r.dir); err != nil {
				panic(fmt.Sprintf("chdir %s. error: %v", r.dir, err))
			}
		}
		go func() {
			ticker := time.NewTicker(time.Second)
			var counter int
			for range ticker.C {
				counter++
				lastBytes := atomic.LoadInt64(&r.lastReadBytes)
				currBytes := atomic.LoadInt64(&r.readBytes)
				lastReadCount := atomic.LoadInt64(&r.lastReadCount)
				currReadCount := atomic.LoadInt64(&r.readCount)
				atomic.StoreInt64(&r.lastReadBytes, currBytes)
				atomic.StoreInt64(&r.lastReadCount, currReadCount)
				fmt.Printf("%06d throughput: [%13s / %13s] iops: [%7d / %7d]\n",
					counter, Size(float64(currBytes)/float64(counter)), Size(currBytes-lastBytes), int64(float64(currReadCount)/float64(counter)), currReadCount-lastReadCount)
			}
		}()
	})

	for p := range pathC {
		n, err := ioutil.ReadFile(string(p))
		if err != nil {
			// fmt.Println(fmt.Sprintf("path: %s  error: %v\n", p, err))
			continue
		}
		atomic.AddInt64(&r.readBytes, int64(len(n)))
		atomic.AddInt64(&r.readCount, 1)
		if r.interval > 0 {
			time.Sleep(r.interval * time.Millisecond)
		}
	}
}

func readRequest(reader io.Reader) <-chan []byte {
	c := make(chan []byte, 4096)

	go func() {
		defer close(c)
		var remains []byte
		reader := bufio.NewReader(reader)
		buffer := make([]byte, 4096)
		for {
			n, err := reader.Read(buffer)
			if err == io.EOF {
				return
			}
			if err != nil {
				fmt.Printf("read error: %v", err)
				return
			}
			remains = append(remains, buffer[0:n]...)
			if index := bytes.IndexByte(remains, '\n'); index == -1 {
				continue
			}

			for {
				index := bytes.IndexByte(remains, '\n')
				if index > 0 {
					c <- remains[:index]
					remains = remains[index+1:]
				} else {
					remains = remains[index+1:]
					break
				}
			}
		}
	}()

	return c
}

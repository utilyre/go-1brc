package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const chunkSize = 128 * 1024 * 1024

type Station struct {
	n             int
	min, max, avg float64
}

func main() {
	defer func(start time.Time) {
		fmt.Fprintln(os.Stderr, "took", time.Since(start))
	}(time.Now())

	chunkc := produceChunks()

	stationcs := make([]<-chan map[string]Station, runtime.NumCPU())
	for i := range runtime.NumCPU() {
		stationcs[i] = produceStationMaps(chunkc)
	}
	stationMapc := Merge(stationcs)

	stationMap := make(map[string]Station)
	for m := range stationMapc {
		for name, station := range m {
			s, _ := stationMap[name]

			s.min = min(s.min, station.min)
			s.max = max(s.max, station.max)
			s.avg = (float64(s.n)*s.avg + float64(station.n)*station.avg) / float64(s.n+station.n)
			s.n += station.n

			stationMap[name] = s
		}
	}

	idx := 0
	names := make([]string, len(stationMap))
	for name := range stationMap {
		names[idx] = name
		idx++
	}
	sort.Strings(names)

	for _, name := range names {
		s := stationMap[name]
		fmt.Printf("%s=%.01f/%.01f/%.01f\n", name, s.min, s.avg, s.max)
	}
}

func Merge[T any](cs []<-chan T) <-chan T {
	mergedc := make(chan T)

	var wg sync.WaitGroup
	wg.Add(len(cs))

	for _, c := range cs {
		go func() {
			defer wg.Done()

			for v := range c {
				mergedc <- v
			}
		}()
	}

	go func() {
		wg.Wait()
		close(mergedc)
	}()

	return mergedc
}

func produceChunks() <-chan []byte {
	chunkc := make(chan []byte)

	go func() {
		defer close(chunkc)

		buf := make([]byte, chunkSize)
		leftover := make([]byte, 0, chunkSize)

		for {
			n, err := os.Stdin.Read(buf)
			if err != nil {
				return
			}

			idx := bytes.LastIndexByte(buf[:n], byte('\n'))
			chunk := append(leftover[:len(leftover):len(leftover)], buf[:idx+1]...)
			leftover = append(leftover[:0], buf[idx+1:n]...)

			chunkc <- chunk
		}
	}()

	return chunkc
}

func produceStationMaps(chunkc <-chan []byte) <-chan map[string]Station {
	stationMapc := make(chan map[string]Station)

	go func() {
		defer close(stationMapc)
		stationMap := make(map[string]Station)

		for chunk := range chunkc {
			scanner := bufio.NewScanner(bytes.NewReader(chunk))

			for scanner.Scan() {
				parts := strings.Split(scanner.Text(), ";")

				name := parts[0]
				temp, _ := strconv.ParseFloat(parts[1], 64)

				station, _ := stationMap[name]
				station.min = min(station.min, temp)
				station.max = max(station.max, temp)
				station.avg = (float64(station.n)*station.avg + temp) / float64(station.n+1)
				station.n++
				stationMap[name] = station
			}
		}

		stationMapc <- stationMap
	}()

	return stationMapc
}

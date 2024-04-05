package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/anthdm/hollywood/actor"
)

const chunkSize = 128 * 1024 * 1024

type Station struct {
	n             int
	min, max, avg float64
}

type Aggregator struct {
	stations map[string]Station
}

func NewAggregator() actor.Receiver {
	return &Aggregator{
		stations: map[string]Station{},
	}
}

func (a *Aggregator) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case []byte:
		ctx.SpawnChildFunc(func(ctx *actor.Context) {
			// process
			_ = msg

			ctx.Engine().Poison(ctx.PID())
		}, "foert")
	}
}

func main() {
	defer func(start time.Time) {
		fmt.Fprintln(os.Stderr, "took", time.Since(start))
	}(time.Now())

	e, err := actor.NewEngine(actor.NewEngineConfig())
	if err != nil {
		log.Fatalln(err)
	}

	pid := e.Spawn(NewAggregator, "agg")

	buf := make([]byte, chunkSize)
	leftover := make([]byte, chunkSize)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			log.Fatalln(err)
		}

		idx := bytes.LastIndex(buf[:n], []byte("\n"))
		if idx == -1 {
			idx = len(buf)
		}
		idx = min(len(buf), idx+1)

		finalBuf := append(leftover, buf[:idx]...)
		copy(leftover, buf[idx:])

		e.Send(pid, finalBuf)
	}
}

// func stupid(r io.Reader) {
// 	// rows=1_000_000 cap=0        ~230ms
// 	// rows=1_000_000 cap=1_1000   ~200ms
// 	// rows=1_000_000 cap=10_1000  ~200ms
// 	// rows=1_000_000 cap=10-_1000 ~180ms
// 	stations := make(map[string]Station, 100_000)
//
// 	n := 0
// 	for scanner := bufio.NewScanner(r); scanner.Scan(); n++ {
// 		line := scanner.Text()
// 		parts := strings.Split(line, ";")
//
// 		name := parts[0]
// 		temp, _ := strconv.ParseFloat(parts[1], 64)
//
// 		station, _ := stations[name]
// 		station.n++
// 		station.min = min(station.min, temp)
// 		station.max = max(station.max, temp)
// 		station.avg += temp / float64(station.n)
// 		stations[name] = station
// 	}
//
// 	names := make([]string, 0, len(stations))
// 	for name := range stations {
// 		names = append(names, name)
// 	}
// 	sort.Strings(names)
//
// 	for _, name := range names {
// 		s := stations[name]
// 		fmt.Printf("%s=%.01f/%.01f/%.01f\n", name, s.min, s.avg, s.max)
// 	}
// }

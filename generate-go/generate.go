package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"time"
	"unicode"

	"github.com/BurntSushi/toml"
)

func main() {
	flag.Parse()

	// Read config
	conf, err := ConfigFromFileTOML(*flagConfigFilePath)
	try("reading config file", err)

	// Prepare
	start := time.Now()
	outFile, err := os.OpenFile(
		*flagOutputFilePath,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0777,
	)
	try("opening output file", err)

	aggrOutFile, err := os.OpenFile(
		*flagAggregateOutputFilePath,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0777,
	)
	try("opening aggregate output file", err)

	out := bufio.NewWriter(outFile)
	aggrOut := bufio.NewWriter(aggrOutFile)

	// Generate
	aggregate, written, err := generate(conf, outFile)
	try("generating", err)

	// Finalize
	try("flushing output file buffer", out.Flush())
	try("syncing output file", outFile.Sync())
	log.Printf(
		"%d bytes written to %s (%s)",
		written,
		*flagOutputFilePath,
		time.Since(start),
	)

	// Write aggregate file
	jsonEnc := json.NewEncoder(aggrOut)
	jsonEnc.SetIndent("", "  ")

	try("writing aggregate file", jsonEnc.Encode(aggregate))
	try("flushing aggregate output file buffer", aggrOut.Flush())
	try("syncing aggregate output file", aggrOutFile.Sync())
	log.Printf("aggregate file written to %s", *flagAggregateOutputFilePath)
}

func try(format string, err error) {
	if err == nil {
		return
	}
	log.Fatalf(format+": %s", err)
}

var (
	flagConfigFilePath = flag.String(
		"c",
		"./generate-conf.toml",
		"generator configuration TOML file path",
	)
	flagOutputFilePath = flag.String(
		"o",
		"./out.txt",
		"output file path",
	)
	flagAggregateOutputFilePath = flag.String(
		"a",
		"./aggregate.json",
		"aggregate output file path",
	)
)

// ConfigFromFileTOML reads the config from a TOML file
func ConfigFromFileTOML(path string) (*Config, error) {
	c := &Config{}
	if _, err := toml.DecodeFile(path, c); err != nil {
		return nil, fmt.Errorf("parsing file: %w", err)
	}
	if err := c.Prepare(); err != nil {
		return nil, err
	}
	return c, nil
}

// Config defines the generator configuration
type Config struct {
	TimeSeed   bool     `toml:"time-seed"`
	RandomSeed int64    `toml:"random-seed"`
	Labels     []string `toml:"labels"`
	MinValues  uint64   `toml:"min-values"`
	MaxValues  uint64   `toml:"max-values"`
	MinVal     int32    `toml:"min-val"`
	MaxVal     int32    `toml:"max-val"`
	Delimiters []string `toml:"delimiters"`
	Separators []string `toml:"separators"`

	labels     [][]byte
	delimiters [][]byte
	separators [][]byte
}

// Prepare verifies and prepares the configuration for use
func (c *Config) Prepare() error {
	// Verify
	switch {
	case c.MinValues < 1:
		return fmt.Errorf(
			"max-values (%d) too small",
			c.MinValues,
		)
	case c.MaxValues < c.MinValues:
		return fmt.Errorf(
			"max-values (%d) smaller min-values (%d)",
			c.MinValues,
			c.MaxValues,
		)
	case c.MaxVal < c.MinVal:
		return fmt.Errorf(
			"max-val (%d) smaller min-val (%d)",
			c.MaxVal,
			c.MinVal,
		)
	case len(c.Labels) < 1:
		return errors.New("missing labels")
	}

	// Prepare
	if len(c.Delimiters) < 1 {
		c.Delimiters = []string{" = "}
	}
	if len(c.Separators) < 1 {
		c.Separators = []string{"; "}
	}

	// Validate delimiters
	delimiters := make(map[string]struct{}, len(c.Delimiters))
	c.delimiters = make([][]byte, 0, len(c.Delimiters))
	for i, d := range c.Delimiters {
		if d == "" {
			return fmt.Errorf("invalid delimiter (empty) at index %d", i)
		}
		if _, ok := delimiters[d]; ok {
			// Duplicate
			return fmt.Errorf("duplicate delimiter (%q) at index %d", d, i)
		}
		delimiters[d] = struct{}{}
		c.delimiters = append(c.delimiters, []byte(d))
	}

	// Validate labels
	labels := make(map[string]struct{}, len(c.Labels))
	c.labels = make([][]byte, 0, len(c.Labels))
	for i, l := range c.Labels {
		if l == "" {
			return fmt.Errorf("invalid label (empty) at index %d", i)
		}
		if _, ok := labels[l]; ok {
			// Duplicate
			return fmt.Errorf("duplicate label (%q) at index %d", l, i)
		}
		for _, c := range l {
			// Labels must not contain space characters
			if unicode.IsSpace(c) {
				return fmt.Errorf("label at index %d contains spaces", i)
			}
		}
		labels[l] = struct{}{}
		c.labels = append(c.labels, []byte(l))
	}

	// Validate separators
	separators := make(map[string]struct{}, len(c.Separators))
	c.separators = make([][]byte, 0, len(c.Separators))
	for i, s := range c.Separators {
		if s == "" {
			return fmt.Errorf("invalid separator (empty) at index %d", i)
		}
		if _, ok := separators[s]; ok {
			// Duplicate
			return fmt.Errorf("duplicate separator (%q) at index %d", s, i)
		}
		separators[s] = struct{}{}
		c.separators = append(c.separators, []byte(s))
	}

	return nil
}

// generate writes a random separated value list to the given output writer
func generate(conf *Config, out io.Writer) (
	aggregate map[string]Aggregate,
	writtenBytes int,
	err error,
) {
	if conf.TimeSeed {
		rand.Seed(time.Now().Unix())
	} else {
		rand.Seed(conf.RandomSeed)
	}
	vals := random(conf.MinValues, conf.MaxValues)

	tmpAggr := make(map[int]int64, len(conf.Labels))
	counters := make([]uint64, len(conf.Labels))
	for i := range conf.Labels {
		tmpAggr[i] = 0
	}

	for i := uint64(0); i < vals; i++ {
		delim := conf.delimiters[randomInt(0, len(conf.delimiters)-1)]
		labelIndex := randomInt(0, len(conf.labels)-1)
		label := conf.labels[labelIndex]
		separator := conf.separators[randomInt(0, len(conf.separators)-1)]

		val := randomInt32(conf.MinVal, conf.MaxVal)
		if tmpAggr[labelIndex]+int64(val) > math.MaxInt32 {
			// Negate the integer to avoid overflowing the aggregate
			val = negateI32(val)
		}

		var n int

		// Update aggregate
		tmpAggr[labelIndex] += int64(val)
		counters[labelIndex]++

		// Write label
		if n, err = out.Write(label); err != nil {
			err = fmt.Errorf("writing label: %w", err)
			return
		}
		writtenBytes += n

		// Write delimiter
		if n, err = out.Write(delim); err != nil {
			err = fmt.Errorf("writing delimiter: %w", err)
			return
		}
		writtenBytes += n

		// Write value
		n, err = fmt.Fprintf(out, "%d", val)
		if err != nil {
			err = fmt.Errorf("writing value: %w", err)
			return
		}
		writtenBytes += n

		if i+1 == vals {
			// Last entry
			break
		}

		// Write separator
		if n, err = out.Write(separator); err != nil {
			err = fmt.Errorf("writing separator: %w", err)
			return
		}
		writtenBytes += n
	}

	aggregate = make(map[string]Aggregate, len(tmpAggr))
	for index, value := range tmpAggr {
		aggregate[conf.Labels[index]] = Aggregate{
			Values: counters[index],
			Value:  int32(value),
		}
	}

	return
}

// Aggregate represents the aggregate for a particular label
type Aggregate struct {
	Values uint64 `json:"values"`
	Value  int32  `json:"value"`
}

func random(min, max uint64) uint64 {
	if min == max {
		return min
	}
	const maxInt64 uint64 = 1<<63 - 1
	n := max - min
	if n < maxInt64 {
		return uint64(rand.Int63n(int64(n+1))) + min
	}
	x := rand.Uint64()
	for x > n {
		x = rand.Uint64()
	}
	return x + min
}

func randomInt(min, max int) int {
	return rand.Intn(max-min+1) + min
}

func randomInt32(min, max int32) int32 {
	return rand.Int31n(max-min+1) + min
}

func negateI32(i int32) int32 {
	if i < 1 {
		return i - i*2
	}
	return i
}

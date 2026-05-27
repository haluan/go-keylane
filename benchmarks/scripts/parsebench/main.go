// SPDX-FileCopyrightText: 2026 Haluan Irsad
// SPDX-License-Identifier: AGPL-3.0-only

// parsebench reads go test -bench output and writes a structured baseline JSON file.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type platform struct {
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	CPU        string `json:"cpu"`
	GOMAXPROCS int    `json:"gomaxprocs"`
}

type benchRecord struct {
	Name        string  `json:"name"`
	NsPerOp     float64 `json:"ns_per_op"`
	BytesPerOp  float64 `json:"bytes_per_op"`
	AllocsPerOp float64 `json:"allocs_per_op"`
	Notes       string  `json:"notes,omitempty"`
}

type baseline struct {
	Version    string        `json:"version"`
	GoVersion  string        `json:"go_version"`
	Date       string        `json:"date"`
	Commit     string        `json:"commit"`
	Platform   platform      `json:"platform"`
	Benchmarks []benchRecord `json:"benchmarks"`
}

// BenchmarkName-8    1234567    890 ns/op    12 B/op    1 allocs/op
var benchLine = regexp.MustCompile(`^Benchmark(\S+)\s+\d+\s+([\d.]+)\s+ns/op(?:\s+([\d.]+)\s+B/op)?(?:\s+([\d.]+)\s+allocs/op)?`)

func main() {
	inPath := flag.String("in", "", "bench output file (required)")
	outPath := flag.String("out", "", "baseline JSON path (required)")
	version := flag.String("version", "v0.8.0", "release version label")
	goVersion := flag.String("go", runtime.Version(), "Go toolchain version")
	commit := flag.String("commit", "", "git commit SHA")
	flag.Parse()

	if *inPath == "" || *outPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	f, err := os.Open(*inPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open input: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	records := make(map[string]benchRecord)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		m := benchLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name := "Benchmark" + m[1]
		ns, _ := strconv.ParseFloat(m[2], 64)
		var bytes, allocs float64
		if m[3] != "" {
			bytes, _ = strconv.ParseFloat(m[3], 64)
		}
		if m[4] != "" {
			allocs, _ = strconv.ParseFloat(m[4], 64)
		}
		records[name] = benchRecord{
			Name:        name,
			NsPerOp:     ns,
			BytesPerOp:  bytes,
			AllocsPerOp: allocs,
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read input: %v\n", err)
		os.Exit(1)
	}

	names := make([]string, 0, len(records))
	for n := range records {
		names = append(names, n)
	}
	// stable order for diffs
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	benches := make([]benchRecord, 0, len(names))
	for _, n := range names {
		benches = append(benches, records[n])
	}

	out := baseline{
		Version:   *version,
		GoVersion: *goVersion,
		Date:      time.Now().UTC().Format("2006-01-02"),
		Commit:    *commit,
		Platform: platform{
			OS:         runtime.GOOS,
			Arch:       runtime.GOARCH,
			CPU:        fmt.Sprintf("%d logical CPUs", runtime.NumCPU()),
			GOMAXPROCS: runtime.GOMAXPROCS(0),
		},
		Benchmarks: benches,
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}
	data = append(data, '\n')
	if err := os.WriteFile(*outPath, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write output: %v\n", err)
		os.Exit(1)
	}
}

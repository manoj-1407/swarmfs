// Command analyze reads a SwarmFS experiment results CSV and prints a
// formatted comparison table: static vs adaptive per scenario, with
// improvement percentages for time, throughput, and failure counts.
//
// Usage:
//
//	analyze [flags]
//	  -input  string   CSV file produced by the experiment binary (default "results/results.csv")
//	  -metric string   primary sort metric: time|tput|failures (default "time")
//
// Output example:
//
//	Scenario           │ Static      Adaptive    Δ time  │ Failures (S/A) │ Switches (S/A)
//	baseline           │ 0.234s      0.219s      -6.4%   │ 0 / 0          │ 0 / 0
//	latency_100ms      │ 1.234s      0.891s      -27.8%  │ 3 / 1          │ 2 / 0
//	loss_5pct          │ 2.341s      1.567s      -33.1%  │ 12 / 4         │ 4 / 2
//	bw_cap_1mbps       │ 4.567s      4.123s       -9.7%  │ 2 / 1          │ 1 / 0
//	mixed              │ 3.456s      2.234s      -35.4%  │ 8 / 3          │ 3 / 1
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"text/tabwriter"
)

type row struct {
	Scenario    string
	Adaptive    bool
	TotalTimeS  float64
	TputMbps    float64
	PiecesOK    int64
	Failed      int64
	Switches    int64
}

func main() {
	input := flag.String("input", "results/results.csv", "CSV file from experiment binary")
	flag.Parse()

	f, err := os.Open(*input)
	if err != nil {
		log.Fatalf("open %s: %v", *input, err)
	}
	defer f.Close()

	rows, err := parseCSV(f)
	if err != nil {
		log.Fatalf("parse CSV: %v", err)
	}
	if len(rows) == 0 {
		fmt.Println("no data in", *input)
		return
	}

	// group by scenario, average repetitions
	type aggKey = struct{ scenario string; adaptive bool }
	totals := map[aggKey]*row{}
	counts := map[aggKey]int{}

	for _, r := range rows {
		k := aggKey{r.Scenario, r.Adaptive}
		if totals[k] == nil {
			totals[k] = &row{Scenario: r.Scenario, Adaptive: r.Adaptive}
		}
		totals[k].TotalTimeS += r.TotalTimeS
		totals[k].TputMbps   += r.TputMbps
		totals[k].PiecesOK   += r.PiecesOK
		totals[k].Failed      += r.Failed
		totals[k].Switches    += r.Switches
		counts[k]++
	}

	// build averages and collect unique scenario order
	avgs := map[aggKey]*row{}
	var scenarioOrder []string
	seen := map[string]bool{}
	for _, r := range rows {
		if !seen[r.Scenario] {
			scenarioOrder = append(scenarioOrder, r.Scenario)
			seen[r.Scenario] = true
		}
		k := aggKey{r.Scenario, r.Adaptive}
		if _, ok := avgs[k]; !ok {
			n := float64(counts[k])
			t := totals[k]
			avgs[k] = &row{
				Scenario:   t.Scenario,
				Adaptive:   t.Adaptive,
				TotalTimeS: t.TotalTimeS / n,
				TputMbps:   t.TputMbps / n,
				PiecesOK:   t.PiecesOK / int64(counts[k]),
				Failed:     t.Failed / int64(counts[k]),
				Switches:   t.Switches / int64(counts[k]),
			}
		}
	}

	// print table
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	sep := "│"

	fmt.Fprintf(tw, "%-20s %s %-10s %-10s %-8s %s %-14s %s %-12s\n",
		"Scenario", sep, "Static", "Adaptive", "Δ time", sep, "Failures(S/A)", sep, "Switches(S/A)")
	fmt.Fprintf(tw, "%-20s %s %-10s %-10s %-8s %s %-14s %s %-12s\n",
		"────────────────────", sep, "──────────", "──────────", "────────",
		sep, "──────────────", sep, "────────────")

	for _, scenario := range scenarioOrder {
		ks := aggKey{scenario, false}
		ka := aggKey{scenario, true}
		s := avgs[ks]
		a := avgs[ka]
		if s == nil || a == nil {
			continue
		}

		deltaTime := pct(s.TotalTimeS, a.TotalTimeS)
		sign := ""
		if deltaTime < 0 {
			sign = "-"
			deltaTime = -deltaTime
		} else if deltaTime > 0 {
			sign = "+"
		}

		fmt.Fprintf(tw, "%-20s %s %-10s %-10s %s%-7s %s %2d / %-10d %s %d / %d\n",
			scenario, sep,
			fmt.Sprintf("%.3fs", s.TotalTimeS),
			fmt.Sprintf("%.3fs", a.TotalTimeS),
			sign, fmt.Sprintf("%.1f%%", deltaTime),
			sep, s.Failed, a.Failed,
			sep, s.Switches, a.Switches)
	}
	tw.Flush()

	// summary line
	var totalStime, totalAtime float64
	var totalSfail, totalAfail int64
	for _, scenario := range scenarioOrder {
		ks := aggKey{scenario, false}
		ka := aggKey{scenario, true}
		if s, a := avgs[ks], avgs[ka]; s != nil && a != nil {
			totalStime += s.TotalTimeS
			totalAtime += a.TotalTimeS
			totalSfail += s.Failed
			totalAfail += a.Failed
		}
	}
	fmt.Printf("\nOverall time improvement:    %.1f%%\n", pct(totalStime, totalAtime))
	fmt.Printf("Overall failure reduction:   %.1f%%\n", pct(float64(totalSfail), float64(totalAfail)))
	fmt.Printf("Reps averaged per scenario:  %d\n", averageReps(rows))
}

func pct(base, new float64) float64 {
	if base == 0 {
		return 0
	}
	return (new - base) / base * 100
}

func averageReps(rows []row) int {
	counts := map[string]int{}
	for _, r := range rows {
		if !r.Adaptive {
			counts[r.Scenario]++
		}
	}
	if len(counts) == 0 {
		return 0
	}
	total := 0
	for _, v := range counts {
		total += v
	}
	return int(math.Round(float64(total) / float64(len(counts))))
}

func parseCSV(r io.Reader) ([]row, error) {
	cr := csv.NewReader(r)
	records, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, nil
	}

	// header: scenario,adaptive,file_size_mb,peer_count,latency_ms,loss_pct,
	//         bw_cap_mbps,total_time_s,throughput_mbps,pieces_ok,failed_attempts,peer_switches
	var rows []row
	for _, rec := range records[1:] {
		if len(rec) < 12 {
			continue
		}
		adaptive := rec[1] == "true"
		timeS, _  := strconv.ParseFloat(rec[7], 64)
		tput, _   := strconv.ParseFloat(rec[8], 64)
		pieces, _ := strconv.ParseInt(rec[9], 10, 64)
		failed, _ := strconv.ParseInt(rec[10], 10, 64)
		switches, _ := strconv.ParseInt(rec[11], 10, 64)

		rows = append(rows, row{
			Scenario:   rec[0],
			Adaptive:   adaptive,
			TotalTimeS: timeS,
			TputMbps:   tput,
			PiecesOK:   pieces,
			Failed:     failed,
			Switches:   switches,
		})
	}
	return rows, nil
}

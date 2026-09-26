// Command experiment runs the SwarmFS scheduler comparison suite.
//
// Usage:
//
//	experiment [flags]
//	  -scenario string   scenario name or "all" (default "all")
//	  -reps     int      repetitions per scenario (default 3)
//	  -output   string   CSV result path (default "results/results.csv")
//	  -size     int      file size in MB, overrides scenario default (0 = use scenario default)
//	  -peers    int      peer count, overrides scenario default (0 = use scenario default)
//
// Each scenario runs twice per repetition: once with the static scheduler and
// once with the adaptive scheduler. Results are appended to the CSV file so
// multiple runs can be aggregated.
//
// Example (in the repo root after 'make build'):
//
//	# baseline conditions
//	./experiment -scenario baseline -reps 5 -output results/baseline.csv
//
//	# latency scenario (apply netem first — requires root):
//	sudo scripts/netem/set_latency.sh 100
//	./experiment -scenario latency_100ms -reps 3
//	sudo scripts/netem/reset.sh
//
//	# full suite:
//	./experiment -reps 3
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/manoj-1407/swarmfs/tests/scenarios"
)

func main() {
	scenarioName := flag.String("scenario", "all", `scenario name or "all"`)
	reps         := flag.Int("reps", 3, "repetitions per scenario")
	output       := flag.String("output", "results/results.csv", "CSV output path")
	sizeMB       := flag.Int("size", 0, "override file size in MB (0 = scenario default)")
	peerCount    := flag.Int("peers", 0, "override peer count (0 = scenario default)")
	flag.Parse()

	log.SetPrefix("experiment ")
	log.SetFlags(log.Ltime)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// resolve scenario list
	toRun := selectScenarios(*scenarioName, *sizeMB, *peerCount)
	if len(toRun) == 0 {
		log.Fatalf("unknown scenario %q — valid names: %s", *scenarioName, scenarioNames())
	}

	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "scenario\trep\tmode\ttime(s)\ttput(Mbps)\tpieces\tfailed\tswitches")

	var allResults []scenarios.RunResult

	for _, cfg := range toRun {
		for rep := 1; rep <= *reps; rep++ {
			if ctx.Err() != nil {
				break
			}

			dataRoot, err := os.MkdirTemp("", "swarmfs-exp-*")
			if err != nil {
				log.Printf("mktemp: %v", err)
				continue
			}

			repStart := time.Now()
			static, adaptive, err := scenarios.RunScenario(ctx, cfg, dataRoot)
			_ = os.RemoveAll(dataRoot)

			if err != nil {
				log.Printf("FAIL scenario=%s rep=%d: %v", cfg.Name, rep, err)
				continue
			}

			elapsed := time.Since(repStart)
			log.Printf("scenario=%s rep=%d done in %s", cfg.Name, rep, elapsed.Round(time.Millisecond))

			printRow(tw, static, rep)
			printRow(tw, adaptive, rep)
			tw.Flush()

			allResults = append(allResults, static, adaptive)
		}
	}

	tw.Flush()

	if len(allResults) > 0 {
		if err := scenarios.AppendCSV(*output, allResults...); err != nil {
			log.Fatalf("write CSV: %v", err)
		}
		log.Printf("results → %s (%d rows)", *output, len(allResults))
	}
}

func printRow(tw *tabwriter.Writer, r scenarios.RunResult, rep int) {
	mode := "static"
	if r.Adaptive {
		mode = "adaptive"
	}
	fmt.Fprintf(tw, "%s\t%d\t%s\t%.3f\t%.3f\t%d\t%d\t%d\n",
		r.Scenario, rep, mode,
		r.TotalTimeS, r.ThroughputMbps,
		r.PiecesOK, r.FailedAttempts, r.PeerSwitches)
}

func selectScenarios(name string, sizeMB, peerCount int) []scenarios.ScenarioConfig {
	all := scenarios.DefaultScenarios
	var chosen []scenarios.ScenarioConfig
	if name == "all" {
		chosen = all
	} else {
		for _, s := range all {
			if s.Name == name {
				chosen = []scenarios.ScenarioConfig{s}
				break
			}
		}
	}
	// apply overrides
	for i := range chosen {
		if sizeMB > 0 {
			chosen[i].FileSizeMB = sizeMB
		}
		if peerCount > 0 {
			chosen[i].PeerCount = peerCount
		}
	}
	return chosen
}

func scenarioNames() string {
	names := ""
	for _, s := range scenarios.DefaultScenarios {
		if names != "" {
			names += ", "
		}
		names += s.Name
	}
	return names
}

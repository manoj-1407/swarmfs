// Command peer runs a SwarmFS peer node.
//
// Seed a file:
//   peer seed --file ./bigfile.bin --listen :9001 --tracker localhost:7000
//
// Download a file:
//   peer leech --hash <fileHash> --listen :9002 --tracker localhost:7000 --out ./output.bin
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/manoj-1407/swarmfs/internal/peer"
)

func main() {
	log.SetPrefix("peer ")
	log.SetFlags(log.Ltime)

	if len(os.Args) < 2 {
		log.Fatalf("usage: peer <seed|leech> [flags]")
	}

	switch os.Args[1] {
	case "seed":
		runSeed(os.Args[2:])
	case "leech":
		runLeech(os.Args[2:])
	default:
		log.Fatalf("unknown command %q — use seed or leech", os.Args[1])
	}
}

func runSeed(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	file    := fs.String("file",    "",              "path to the file to seed (required)")
	listen  := fs.String("listen",  ":9001",         "peer listen address")
	trkAddr := fs.String("tracker", "localhost:7000", "tracker address")
	dataDir := fs.String("data",    ".swarmfs",      "piece store directory")
	_ = fs.Parse(args)

	if *file == "" {
		log.Fatal("--file is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	node, err := peer.NewSeedNode(peer.Config{
		ListenAddr:  *listen,
		TrackerAddr: *trkAddr,
		DataDir:     *dataDir,
	}, *file)
	if err != nil {
		log.Fatalf("init seeder: %v", err)
	}

	log.Printf("seeding %s", node.FileHash())
	if err := node.Seed(ctx); err != nil {
		log.Fatalf("seed: %v", err)
	}
}

func runLeech(args []string) {
	fs := flag.NewFlagSet("leech", flag.ExitOnError)
	hash    := fs.String("hash",    "",              "file hash to download (required)")
	out     := fs.String("out",     "./output",      "output file path")
	listen  := fs.String("listen",  ":9002",         "peer listen address")
	trkAddr := fs.String("tracker", "localhost:7000", "tracker address")
	dataDir := fs.String("data",    ".swarmfs",      "piece store directory")
	timeout := fs.Duration("timeout", 5*time.Minute, "download timeout")
	_ = fs.Parse(args)

	if *hash == "" {
		log.Fatal("--hash is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	node, err := peer.NewLeechNode(peer.Config{
		ListenAddr:  *listen,
		TrackerAddr: *trkAddr,
		DataDir:     *dataDir,
	}, *hash)
	if err != nil {
		log.Fatalf("init leecher: %v", err)
	}

	log.Printf("downloading %s → %s", *hash, *out)
	if err := node.Leech(ctx, *out); err != nil {
		log.Fatalf("leech: %v", err)
	}
	log.Println("done")
}

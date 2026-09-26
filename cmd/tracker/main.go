// Command tracker runs the SwarmFS discovery and registration server.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/manoj-1407/swarmfs/internal/tracker"
)

func main() {
	addr := flag.String("addr", ":7000", "listen address")
	flag.Parse()

	log.SetPrefix("tracker ")
	log.SetFlags(log.Ltime)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := tracker.NewServer(*addr)
	log.Printf("listening on %s", *addr)

	if err := srv.Serve(ctx); err != nil {
		log.Fatalf("serve: %v", err)
	}
	log.Println("stopped")
}

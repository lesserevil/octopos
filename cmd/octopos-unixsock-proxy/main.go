package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/octopos/octopos/pkg/unixbroker"
)

func main() {
	listenPath := flag.String("listen", "", "Filesystem Unix socket path to listen on")
	targetPath := flag.String("target", "", "Filesystem Unix socket path to proxy to")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := (unixbroker.Broker{ListenPath: *listenPath, TargetPath: *targetPath}).Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "octopos-unixsock-proxy: %v\n", err)
		os.Exit(1)
	}
}

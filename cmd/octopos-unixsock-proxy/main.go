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
	stdio := flag.Bool("stdio", false, "Proxy stdin/stdout to the target Unix socket instead of listening")
	flag.Parse()

	if *stdio {
		if err := unixbroker.Proxy(*targetPath, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "octopos-unixsock-proxy: %v\n", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := (unixbroker.Broker{ListenPath: *listenPath, TargetPath: *targetPath}).Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "octopos-unixsock-proxy: %v\n", err)
		os.Exit(1)
	}
}

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	orpc "github.com/octopos/octopos/pkg/rpc"
	"github.com/octopos/octopos/pkg/unixbroker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	listenPath := flag.String("listen", "", "Filesystem Unix socket path to listen on")
	targetPath := flag.String("target", "", "Filesystem Unix socket path to proxy to")
	stdio := flag.Bool("stdio", false, "Proxy stdin/stdout to the target Unix socket instead of listening")
	addr := flag.String("addr", "", "octoposd gRPC address for broker operations")
	path := flag.String("path", "", "Cluster Unix socket path for broker operations")
	register := flag.Bool("register", false, "Register --path with the octoposd Unix socket broker")
	unregister := flag.Bool("unregister", false, "Unregister --path from the octoposd Unix socket broker")
	list := flag.Bool("list", false, "List Unix sockets registered with the octoposd broker")
	flag.Parse()

	if *addr != "" {
		if err := runBrokerCommand(*addr, *path, *targetPath, *register, *unregister, *list, *stdio); err != nil {
			fmt.Fprintf(os.Stderr, "octopos-unixsock-proxy: %v\n", err)
			os.Exit(1)
		}
		return
	}

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

func runBrokerCommand(addr string, path string, targetPath string, register bool, unregister bool, list bool, stdio bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("connect octoposd %s: %w", addr, err)
	}
	defer conn.Close()
	client := orpc.NewClusterClient(conn)

	switch {
	case list:
		resp, err := client.ListUnixSockets(context.Background(), &orpc.ListUnixSocketsRequest{})
		if err != nil {
			return err
		}
		for _, socket := range resp.Sockets {
			fmt.Printf("%s\t%s\t%s\n", socket.Path, socket.NodeId, socket.TargetPath)
		}
		return nil
	case register:
		resp, err := client.RegisterUnixSocket(context.Background(), &orpc.RegisterUnixSocketRequest{
			Path:       path,
			TargetPath: targetPath,
		})
		if err != nil {
			return err
		}
		if !resp.Success {
			return errors.New(resp.Error)
		}
		return nil
	case unregister:
		resp, err := client.UnregisterUnixSocket(context.Background(), &orpc.UnregisterUnixSocketRequest{Path: path})
		if err != nil {
			return err
		}
		if !resp.Success {
			return errors.New(resp.Error)
		}
		return nil
	case stdio:
		return proxyBrokerStdio(context.Background(), client, path, os.Stdin, os.Stdout)
	default:
		return fmt.Errorf("with --addr, specify one of --register, --unregister, --list, or --stdio")
	}
}

func proxyBrokerStdio(ctx context.Context, client orpc.ClusterClient, path string, input io.Reader, output io.Writer) error {
	if path == "" {
		return fmt.Errorf("--path is required")
	}
	stream, err := client.UnixSocketStream(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&orpc.UnixSocketFrame{Path: path}); err != nil {
		_ = stream.CloseSend()
		return err
	}
	go func() {
		defer stream.CloseSend()
		buf := make([]byte, 32*1024)
		for {
			n, err := input.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				if sendErr := stream.Send(&orpc.UnixSocketFrame{Data: data}); sendErr != nil {
					return
				}
			}
			if err != nil {
				_ = stream.Send(&orpc.UnixSocketFrame{Close: true})
				return
			}
		}
	}()
	for {
		frame, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if frame.Error != "" {
			return errors.New(frame.Error)
		}
		if len(frame.Data) > 0 {
			if _, err := output.Write(frame.Data); err != nil {
				return err
			}
		}
		if frame.Close {
			return nil
		}
	}
}

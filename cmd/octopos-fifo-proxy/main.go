package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	orpc "github.com/octopos/octopos/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	fifoKeyPrefix        = "fifo-path:"
	coordinatorKeyPrefix = "coord:"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "octoposd gRPC address")
	path := flag.String("path", "", "SSI-root FIFO path")
	mode := flag.String("mode", "", "FIFO endpoint mode: read or write")
	readyFD := flag.Int("ready-fd", -1, "optional fd that receives a 32-bit errno-style readiness status")
	flag.Parse()

	err := run(*addr, *path, *mode, *readyFD, os.Stdin, os.Stdout)
	if err != nil {
		notifyReady(*readyFD, syscall.EIO)
		fmt.Fprintf(os.Stderr, "octopos-fifo-proxy: %v\n", err)
		os.Exit(1)
	}
}

func run(addr, path, mode string, readyFD int, input io.Reader, output io.Writer) error {
	fd, err := fifoModeFD(mode)
	if err != nil {
		return err
	}
	key, err := fifoPipeKey(path, os.Getenv("OCTOPOS_SESSION_ID"), firstNonEmpty(os.Getenv("OCTOPOS_PARENT_JOB_ID"), os.Getenv("OCTOPOS_JOB_ID")), os.Getenv("OCTOPOS_REMOTE_CHILD_PIPE_COORD_NODE"))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	cancel()
	if err != nil {
		return fmt.Errorf("connect octoposd %s: %w", addr, err)
	}
	defer conn.Close()

	stream, err := orpc.NewClusterClient(conn).PipeStream(context.Background())
	if err != nil {
		return err
	}
	if err := stream.Send(&orpc.PipeFrame{Key: key, Fd: int32(fd)}); err != nil {
		_ = stream.CloseSend()
		return err
	}
	ack, err := stream.Recv()
	if err != nil {
		_ = stream.CloseSend()
		return err
	}
	if ack.GetError() != "" {
		_ = stream.CloseSend()
		return errors.New(ack.GetError())
	}
	notifyReady(readyFD, 0)

	switch fd {
	case 0:
		return fifoStreamToWriter(stream, output)
	case 1:
		return readerToFIFOStream(input, stream)
	default:
		_ = stream.CloseSend()
		return fmt.Errorf("unsupported FIFO fd %d", fd)
	}
}

func fifoModeFD(mode string) (int, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "read":
		return 0, nil
	case "write":
		return 1, nil
	default:
		return -1, fmt.Errorf("--mode must be read or write")
	}
}

func fifoPipeKey(path, sessionID, jobID, coordinatorNode string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("--path is required")
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("FIFO path %q must be absolute", path)
	}
	clean := filepath.Clean(path)
	if sessionID == "" {
		sessionID = "unknown-session"
	}
	if jobID == "" {
		jobID = "unknown-job"
	}
	key := sessionID + "\x00" + jobID + "\x00" + fifoKeyPrefix + clean
	if coordinatorNode != "" {
		key = coordinatorKeyPrefix + coordinatorNode + "\x00" + key
	}
	return key, nil
}

func fifoStreamToWriter(stream orpc.Cluster_PipeStreamClient, output io.Writer) error {
	defer stream.CloseSend()
	for {
		frame, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if frame.GetError() != "" {
			return errors.New(frame.GetError())
		}
		if len(frame.GetData()) > 0 {
			if _, err := output.Write(frame.GetData()); err != nil {
				_ = stream.Send(&orpc.PipeFrame{Error: err.Error(), Close: true})
				return err
			}
		}
		if frame.GetClose() {
			return nil
		}
	}
}

func readerToFIFOStream(input io.Reader, stream orpc.Cluster_PipeStreamClient) error {
	defer stream.CloseSend()
	buf := make([]byte, 32*1024)
	for {
		n, err := input.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := stream.Send(&orpc.PipeFrame{Data: data}); sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			_ = stream.Send(&orpc.PipeFrame{Close: true})
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func notifyReady(fd int, code syscall.Errno) {
	if fd < 0 {
		return
	}
	var buf [4]byte
	value := int32(code)
	buf[0] = byte(value)
	buf[1] = byte(value >> 8)
	buf[2] = byte(value >> 16)
	buf[3] = byte(value >> 24)
	_, _ = syscall.Write(fd, buf[:])
	_ = syscall.Close(fd)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

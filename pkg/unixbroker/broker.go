package unixbroker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
)

type Broker struct {
	ListenPath string
	TargetPath string
}

func Proxy(targetPath string, input io.Reader, output io.Writer) error {
	if targetPath == "" {
		return errors.New("missing target path")
	}
	target, err := net.Dial("unix", targetPath)
	if err != nil {
		return fmt.Errorf("dial unix %s: %w", targetPath, err)
	}
	defer target.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(target, input)
		closeWrite(target)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(output, target)
	}()
	wg.Wait()
	return nil
}

func (b Broker) Serve(ctx context.Context) error {
	if b.ListenPath == "" {
		return errors.New("missing listen path")
	}
	if b.TargetPath == "" {
		return errors.New("missing target path")
	}
	if err := prepareListenPath(b.ListenPath); err != nil {
		return err
	}
	listener, err := net.Listen("unix", b.ListenPath)
	if err != nil {
		return fmt.Errorf("listen unix %s: %w", b.ListenPath, err)
	}
	defer listener.Close()
	defer os.Remove(b.ListenPath)

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept unix %s: %w", b.ListenPath, err)
		}
		go b.handle(conn)
	}
}

func (b Broker) handle(client net.Conn) {
	defer client.Close()
	target, err := net.Dial("unix", b.TargetPath)
	if err != nil {
		return
	}
	defer target.Close()
	proxyPair(client, target)
}

func proxyPair(left net.Conn, right net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go proxyOneWay(&wg, left, right)
	go proxyOneWay(&wg, right, left)
	wg.Wait()
}

func proxyOneWay(wg *sync.WaitGroup, dst net.Conn, src net.Conn) {
	defer wg.Done()
	_, _ = io.Copy(dst, src)
	closeWrite(dst)
}

func closeWrite(conn net.Conn) {
	if c, ok := conn.(*net.UnixConn); ok {
		_ = c.CloseWrite()
	}
}

func prepareListenPath(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create unix socket directory: %w", err)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat unix socket %s: %w", path, err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to replace non-socket %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale unix socket %s: %w", path, err)
	}
	return nil
}

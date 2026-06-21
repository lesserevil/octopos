package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type backend struct {
	raw     string
	url     *url.URL
	healthy atomic.Bool
}

type proxyState struct {
	backends []*backend
	next     atomic.Uint64
}

func main() {
	var listen string
	var targetsRaw string
	var healthPath string
	var healthInterval time.Duration
	var healthTimeout time.Duration
	flag.StringVar(&listen, "listen", "127.0.0.1:19000", "listen address")
	flag.StringVar(&targetsRaw, "targets", "10.0.0.1:9000,10.0.0.2:9000,10.0.0.3:9000", "comma-separated MinIO target addresses")
	flag.StringVar(&healthPath, "health-path", "/minio/health/cluster", "backend health check path")
	flag.DurationVar(&healthInterval, "health-interval", 2*time.Second, "backend health check interval")
	flag.DurationVar(&healthTimeout, "health-timeout", 1500*time.Millisecond, "backend health check timeout")
	flag.Parse()

	backends, err := parseBackends(targetsRaw)
	if err != nil {
		log.Fatal(err)
	}
	state := &proxyState{backends: backends}
	for _, b := range backends {
		if checkBackend(b, healthPath, healthTimeout) {
			b.healthy.Store(true)
		}
		go watchBackend(b, healthPath, healthInterval, healthTimeout)
	}

	handler := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			target := state.pick()
			if target == nil {
				return
			}
			req.URL.Scheme = target.url.Scheme
			req.URL.Host = target.url.Host
			req.URL.Path = singleJoiningSlash(target.url.Path, req.URL.Path)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadGateway)
		},
		ModifyResponse: func(resp *http.Response) error {
			return nil
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/octopos-objectstore-proxy/healthz", func(w http.ResponseWriter, r *http.Request) {
		if state.countHealthy() == 0 {
			http.Error(w, "no healthy object-store backends", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if state.countHealthy() == 0 {
			http.Error(w, "no healthy object-store backends", http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s with %d backends", listen, len(backends))
		errCh <- server.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}
}

func parseBackends(raw string) ([]*backend, error) {
	var backends []*backend
	for _, part := range strings.Split(raw, ",") {
		addr := strings.TrimSpace(part)
		if addr == "" {
			continue
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid target %q: %w", addr, err)
		}
		if host == "" || port == "" {
			return nil, fmt.Errorf("invalid target %q", addr)
		}
		u, err := url.Parse("http://" + addr)
		if err != nil {
			return nil, err
		}
		backends = append(backends, &backend{raw: addr, url: u})
	}
	if len(backends) == 0 {
		return nil, fmt.Errorf("no targets configured")
	}
	return backends, nil
}

func watchBackend(b *backend, healthPath string, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		b.healthy.Store(checkBackend(b, healthPath, timeout))
	}
}

func checkBackend(b *backend, healthPath string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.url.String()+healthPath, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (s *proxyState) pick() *backend {
	count := uint64(len(s.backends))
	if count == 0 {
		return nil
	}
	start := s.next.Add(1)
	for i := uint64(0); i < count; i++ {
		idx := (start + i) % count
		candidate := s.backends[idx]
		if candidate.healthy.Load() {
			return candidate
		}
	}
	return nil
}

func (s *proxyState) countHealthy() int {
	var count int
	for _, b := range s.backends {
		if b.healthy.Load() {
			count++
		}
	}
	return count
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	default:
		return a + b
	}
}

package ebpf

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
)

type EventCallback func(eventType uint32, data []byte)

type Loader struct {
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	programs map[ProgramType]*loadedProgram
	eventCb  EventCallback
}

type loadedProgram struct {
	typ    ProgramType
	coll   *ebpf.Collection
	links  []link.Link
	reader *ringbuf.Reader
}

func NewLoader(ctx context.Context) (*Loader, error) {
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("remove memlock: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	return &Loader{
		ctx:      ctx,
		cancel:   cancel,
		programs: make(map[ProgramType]*loadedProgram),
	}, nil
}

func (l *Loader) SetEventCallback(cb EventCallback) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.eventCb = cb
}

func (l *Loader) Load(typ ProgramType, objPath string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.programs[typ]; ok {
		return fmt.Errorf("program %s already loaded", typ)
	}

	spec, err := ebpf.LoadCollectionSpec(objPath)
	if err != nil {
		return fmt.Errorf("load spec %s: %w", typ, err)
	}

	coll, err := ebpf.NewCollectionWithOptions(spec, ebpf.CollectionOptions{})
	if err != nil {
		return fmt.Errorf("load collection %s: %w", typ, err)
	}

	l.programs[typ] = &loadedProgram{typ: typ, coll: coll}
	return nil
}

func (l *Loader) LoadAuto(typ ProgramType) error {
	objDir := filepath.Join("ebpf", typ.String())
	objPath := filepath.Join(objDir, typ.String()+".bpf.o")

	if _, err := os.Stat(objPath); os.IsNotExist(err) {
		return fmt.Errorf("object file %s not found; run 'make' in %s first", objPath, objDir)
	}
	return l.Load(typ, objPath)
}

func (l *Loader) AttachTracepoint(typ ProgramType, tracepoint string, progName string) error {
	l.mu.Lock()
	prog, ok := l.programs[typ]
	l.mu.Unlock()
	if !ok {
		return fmt.Errorf("program %s not loaded", typ)
	}

	p, ok := prog.coll.Programs[progName]
	if !ok {
		return fmt.Errorf("program %s not found in %s collection", progName, typ)
	}

	group, event := splitTracepoint(tracepoint)

	lnk, err := link.Tracepoint(group, event, p, nil)
	if err != nil {
		return fmt.Errorf("attach tracepoint %s: %w", tracepoint, err)
	}

	prog.links = append(prog.links, lnk)
	return nil
}

func (l *Loader) StartRingbuf(typ ProgramType, mapName string) error {
	l.mu.Lock()
	prog, ok := l.programs[typ]
	l.mu.Unlock()
	if !ok {
		return fmt.Errorf("program %s not loaded", typ)
	}

	m, ok := prog.coll.Maps[mapName]
	if !ok {
		return fmt.Errorf("map %s not found in %s collection", mapName, typ)
	}

	reader, err := ringbuf.NewReader(m)
	if err != nil {
		return fmt.Errorf("ringbuf reader %s: %w", mapName, err)
	}
	prog.reader = reader

	go l.processRingbuf(typ, reader)
	return nil
}

func (l *Loader) processRingbuf(typ ProgramType, reader *ringbuf.Reader) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("ringbuf reader for %s panicked: %v", typ, r)
		}
	}()

	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, ringbuf.ErrClosed) {
				return
			}
			log.Printf("ringbuf read error %s: %v", typ, err)
			continue
		}

		if len(record.RawSample) < 16 {
			continue
		}

		eventType := hostByteOrder.Uint32(record.RawSample[8:12])

		l.mu.Lock()
		cb := l.eventCb
		l.mu.Unlock()

		if cb != nil {
			cb(eventType, record.RawSample)
		}
	}
}

func (l *Loader) Stop(typ ProgramType) error {
	l.mu.Lock()
	prog, ok := l.programs[typ]
	delete(l.programs, typ)
	l.mu.Unlock()

	if !ok {
		return nil
	}

	var errs []error

	if prog.reader != nil {
		if err := prog.reader.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close reader: %w", err))
		}
	}

	for _, lnk := range prog.links {
		if err := lnk.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close link: %w", err))
		}
	}

	if prog.coll != nil {
		prog.coll.Close()
	}

	return errors.Join(errs...)
}

func (l *Loader) Close() error {
	l.cancel()

	l.mu.Lock()
	types := make([]ProgramType, 0, len(l.programs))
	for t := range l.programs {
		types = append(types, t)
	}
	l.mu.Unlock()

	var errs []error
	for _, t := range types {
		if err := l.Stop(t); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (l *Loader) Map(typ ProgramType, name string) *ebpf.Map {
	l.mu.Lock()
	defer l.mu.Unlock()

	prog, ok := l.programs[typ]
	if !ok {
		return nil
	}
	return prog.coll.Maps[name]
}

func splitTracepoint(tp string) (string, string) {
	for i, c := range tp {
		if c == '/' {
			return tp[:i], tp[i+1:]
		}
	}
	return tp, ""
}

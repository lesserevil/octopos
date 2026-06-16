package ebpf

import (
	"encoding/binary"
	"fmt"
)

type ProgramType int

const (
	ProgramProcAggregator ProgramType = iota
	ProgramSysAggregator
	ProgramDevProxy
	ProgramPipeSplice
)

func (p ProgramType) String() string {
	switch p {
	case ProgramProcAggregator:
		return "proc_aggregator"
	case ProgramSysAggregator:
		return "sys_aggregator"
	case ProgramDevProxy:
		return "dev_proxy"
	case ProgramPipeSplice:
		return "pipe_splice"
	default:
		return "unknown"
	}
}

func (p ProgramType) ObjectPath() string {
	return fmt.Sprintf("ebpf/%s/%s.bpf.o", p.String(), p.String())
}

var hostByteOrder = binary.LittleEndian

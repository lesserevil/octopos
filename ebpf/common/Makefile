# eBPF Build Configuration

CC = clang
ARCH := $(shell uname -m | sed 's/x86_64/x86/; s/aarch64/arm64/; s/arm64/arm64/')
CFLAGS = -O2 -g -Wall -target bpf -D__TARGET_ARCH_$(ARCH) -I./include -I/usr/include
LDFLAGS = -lelf -lz

# Kernel headers path (adjust for your system)
KERNEL_HEADERS = /usr/src/linux-headers-$(shell uname -r)
BPF_HEADERS = /usr/include/bpf

all: proc_aggregator.bpf.o sys_aggregator.bpf.o dev_proxy.bpf.o pipe_splice.bpf.o

%.bpf.o: src/%.bpf.c
	$(CC) $(CFLAGS) -I$(KERNEL_HEADERS) -I$(BPF_HEADERS) -c $< -o $@

clean:
	rm -f *.bpf.o

verify: proc_aggregator.bpf.o
	bpftool prog load $< /sys/fs/bpf/test type tracepoint 2>&1 | head -20

.PHONY: all clean verify
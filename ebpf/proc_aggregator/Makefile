# eBPF Build Configuration

CC = clang
ARCH := $(shell uname -m | sed 's/x86_64/x86/; s/aarch64/arm64/; s/arm64/arm64/')
CFLAGS = -O2 -g -Wall -target bpf -D__TARGET_ARCH_$(ARCH)
COMMON_INCLUDE = -I../../common/include
KERNEL_HEADERS = /usr/src/linux-headers-$(shell uname -r)
KERNEL_INCLUDE = -I$(KERNEL_HEADERS)/include -I$(KERNEL_HEADERS)/arch/$(ARCH)/include -I$(KERNEL_HEADERS)/arch/$(ARCH)/include/generated
BPF_HEADERS = -I/usr/include/bpf

all: proc_aggregator.bpf.o

proc_aggregator.bpf.o: src/proc_aggregator.bpf.c
	$(CC) $(CFLAGS) $(COMMON_INCLUDE) $(KERNEL_INCLUDE) $(BPF_HEADERS) -c $< -o $@

clean:
	rm -f *.bpf.o

verify: proc_aggregator.bpf.o
	bpftool prog load $< /sys/fs/bpf/test type tracepoint 2>&1 | head -20

.PHONY: all clean verify

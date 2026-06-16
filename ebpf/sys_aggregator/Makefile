CC = clang
ARCH := $(shell uname -m | sed 's/x86_64/x86/; s/aarch64/arm64/; s/arm64/arm64/')
CFLAGS = -O2 -g -Wall -target bpf -D__TARGET_ARCH_$(ARCH)
COMMON_INCLUDE = -I../../common/include
KERNEL_HEADERS = /usr/src/linux-headers-$(shell uname -r)
KERNEL_INCLUDE = -I$(KERNEL_HEADERS)/include -I$(KERNEL_HEADERS)/arch/$(ARCH)/include -I$(KERNEL_HEADERS)/arch/$(ARCH)/include/generated
BPF_HEADERS = -I/usr/include/bpf

all: sys_aggregator.bpf.o

sys_aggregator.bpf.o: src/sys_aggregator.bpf.c
	$(CC) $(CFLAGS) $(COMMON_INCLUDE) $(KERNEL_INCLUDE) $(BPF_HEADERS) -c $< -o $@

clean:
	rm -f *.bpf.o

.PHONY: all clean

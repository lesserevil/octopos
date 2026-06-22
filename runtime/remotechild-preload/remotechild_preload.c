#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/fanotify.h>
#include <sys/inotify.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/mman.h>
#include <sys/ptrace.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

#ifndef AT_EMPTY_PATH
#define AT_EMPTY_PATH 0x1000
#endif

#ifndef MAP_TYPE
#define MAP_TYPE 0x0f
#endif

#ifndef MAP_SHARED_VALIDATE
#define MAP_SHARED_VALIDATE MAP_SHARED
#endif

#ifndef SOCK_TYPE_MASK
#define SOCK_TYPE_MASK 0xf
#endif

extern char **environ;

static const char *remote_child_path = "/usr/local/bin/octopos-remote-child";
static const char *remote_child_path_env = "OCTOPOS_REMOTE_CHILD_PATH";
static const char *unixsock_proxy_path = "/usr/local/bin/octopos-unixsock-proxy";
static const char *unixsock_proxy_path_env = "OCTOPOS_UNIXSOCK_PROXY_PATH";
static const char *fifo_proxy_path = "/usr/local/bin/octopos-fifo-proxy";
static const char *fifo_proxy_path_env = "OCTOPOS_FIFO_PROXY_PATH";
static const char *active_env = "OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE=1";
static const char *ipc_compat_env = "OCTOPOS_REMOTE_IPC_COMPAT";
static const char *ipc_compat_warned_env = "OCTOPOS_REMOTE_IPC_COMPAT_WARNED";
static const char *ipc_compat_block_warned_env = "OCTOPOS_REMOTE_IPC_BLOCK_WARNED";
static const char *fifo_warned_env = "OCTOPOS_REMOTE_FIFO_WARNED";
static const char *kernel_ipc_warned_env = "OCTOPOS_REMOTE_KERNEL_IPC_WARNED";
static const char *unixsock_warned_env = "OCTOPOS_REMOTE_UNIXSOCK_WARNED";
static const char *mode_env = "OCTOPOS_REMOTE_CHILDREN";

static int has_prefix(const char *s, const char *prefix) {
    return s != NULL && strncmp(s, prefix, strlen(prefix)) == 0;
}

static const char *env_value(char *const envp[], const char *name) {
    size_t n = strlen(name);
    char **env = envp != NULL ? (char **)envp : environ;
    if (env == NULL) {
        return NULL;
    }
    for (size_t i = 0; env[i] != NULL; i++) {
        if (strncmp(env[i], name, n) == 0 && env[i][n] == '=') {
            return env[i] + n + 1;
        }
    }
    return NULL;
}

static int remoting_enabled(char *const envp[]) {
    const char *mode = env_value(envp, mode_env);
    if (mode == NULL || mode[0] == '\0' || strcmp(mode, "off") == 0 || strcmp(mode, "0") == 0) {
        return 0;
    }
    if (strcmp(mode, "safe") != 0 && strcmp(mode, "aggressive") != 0 && strcmp(mode, "1") != 0) {
        return 0;
    }
    const char *active = env_value(envp, "OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE");
    if (active != NULL && strcmp(active, "1") == 0) {
        return 0;
    }
    const char *remote_child = env_value(envp, "OCTOPOS_REMOTE_CHILD");
    if (remote_child != NULL && strcmp(remote_child, "1") == 0) {
        return 0;
    }
    return 1;
}

static int remote_child_process(void) {
    const char *remote_child = getenv("OCTOPOS_REMOTE_CHILD");
    return remote_child != NULL && strcmp(remote_child, "1") == 0;
}

static int fifo_proxy_enabled(void) {
    const char *active = getenv("OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE");
    if (active != NULL && strcmp(active, "1") == 0) {
        return 0;
    }
    if (remote_child_process()) {
        return 1;
    }
    const char *mode = getenv(mode_env);
    if (mode == NULL || mode[0] == '\0' || strcmp(mode, "off") == 0 || strcmp(mode, "0") == 0) {
        return 0;
    }
    return strcmp(mode, "safe") == 0 || strcmp(mode, "aggressive") == 0 || strcmp(mode, "1") == 0;
}

static const char *remote_child_binary(void) {
    const char *override = getenv(remote_child_path_env);
    if (override != NULL && override[0] != '\0') {
        return override;
    }
    return remote_child_path;
}

static const char *unixsock_proxy_binary(void) {
    const char *override = getenv(unixsock_proxy_path_env);
    if (override != NULL && override[0] != '\0') {
        return override;
    }
    return unixsock_proxy_path;
}

static const char *fifo_proxy_binary(void) {
    const char *override = getenv(fifo_proxy_path_env);
    if (override != NULL && override[0] != '\0') {
        return override;
    }
    return fifo_proxy_path;
}

static const char *broker_addr(void) {
    const char *addr = getenv("OCTOPOS_BROKER_ADDR");
    if (addr != NULL && addr[0] != '\0') {
        return addr;
    }
    return "127.0.0.1:50051";
}

static int shared_mapping(int flags) {
    int map_type = flags & MAP_TYPE;
    return map_type == MAP_SHARED || map_type == MAP_SHARED_VALIDATE;
}

static int private_mapping_flags(int flags) {
    return (flags & ~MAP_TYPE) | MAP_PRIVATE;
}

static int read_fd_path(int fd, char *buf, size_t len) {
    if (len == 0) {
        return -1;
    }
    char proc_path[64];
    int written = snprintf(proc_path, sizeof(proc_path), "/proc/self/fd/%d", fd);
    if (written < 0 || (size_t)written >= sizeof(proc_path)) {
        return -1;
    }
    ssize_t n = readlink(proc_path, buf, len - 1);
    if (n < 0) {
        return -1;
    }
    buf[n] = '\0';
    return 0;
}

static int fd_is_regular_file(int fd) {
    struct stat st;
    if (fstat(fd, &st) != 0) {
        return 0;
    }
    return S_ISREG(st.st_mode);
}

static int fd_is_relaxed_mmap_file(int fd) {
    if (fd < 0 || !fd_is_regular_file(fd)) {
        return 0;
    }

    char path[4096];
    if (read_fd_path(fd, path, sizeof(path)) != 0) {
        return 0;
    }
    if (has_prefix(path, "/dev/shm/") || has_prefix(path, "/run/shm/") ||
        has_prefix(path, "/proc/") || has_prefix(path, "/sys/") ||
        has_prefix(path, "/memfd:") || has_prefix(path, "memfd:") ||
        strstr(path, " (deleted)") != NULL) {
        return 0;
    }
    return 1;
}

static const char *blocked_mmap_reason(int prot, int flags, int fd) {
    if ((prot & PROT_WRITE) != 0) {
        return "writable MAP_SHARED";
    }
    if ((flags & MAP_ANONYMOUS) != 0) {
        return "anonymous MAP_SHARED";
    }
    if (fd < 0) {
        return "non-file MAP_SHARED";
    }

    char path[4096];
    if (read_fd_path(fd, path, sizeof(path)) == 0) {
        if (has_prefix(path, "/dev/shm/") || has_prefix(path, "/run/shm/") ||
            has_prefix(path, "/memfd:") || has_prefix(path, "memfd:")) {
            return "shared-memory MAP_SHARED";
        }
        if (has_prefix(path, "/proc/")) {
            return "procfs MAP_SHARED";
        }
        if (has_prefix(path, "/sys/")) {
            return "sysfs MAP_SHARED";
        }
        if (strstr(path, " (deleted)") != NULL) {
            return "deleted-file MAP_SHARED";
        }
    }
    if (!fd_is_regular_file(fd)) {
        return "non-regular-file MAP_SHARED";
    }
    return "unsupported MAP_SHARED";
}

static void warn_once(const char *env_key, const char *message, const char *detail) {
    const char *warned = getenv(env_key);
    if (warned != NULL && strcmp(warned, "1") == 0) {
        return;
    }
    setenv(env_key, "1", 1);
    if (detail != NULL && detail[0] != '\0') {
        fprintf(stderr, "%s: %s\n", message, detail);
    } else {
        fprintf(stderr, "%s\n", message);
    }
}

static int unsupported_unix_socket(const char *detail) {
    warn_once(unixsock_warned_env,
              "octopos-remote-child: unsupported Unix socket operation blocked in remote child",
              detail);
    errno = ENOTSUP;
    return -1;
}

static int unsupported_fifo(const char *detail) {
    warn_once(fifo_warned_env,
              "octopos-remote-child: unsupported FIFO operation blocked in remote child",
              detail);
    errno = ENOTSUP;
    return -1;
}

static int unsupported_kernel_ipc(const char *detail) {
    warn_once(kernel_ipc_warned_env,
              "octopos-remote-child: unsupported local-kernel IPC blocked in remote child",
              detail);
    errno = ENOTSUP;
    return -1;
}

static void *unsupported_kernel_ipc_ptr(const char *detail) {
    warn_once(kernel_ipc_warned_env,
              "octopos-remote-child: unsupported local-kernel IPC blocked in remote child",
              detail);
    errno = ENOTSUP;
    return (void *)-1;
}

static int socket_type_value(int type) {
    return type & SOCK_TYPE_MASK;
}

static int fd_socket_type(int fd) {
    int value = 0;
    socklen_t len = sizeof(value);
    if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &value, &len) != 0) {
        return -1;
    }
    return socket_type_value(value);
}

static int fd_socket_domain(int fd) {
    int value = 0;
    socklen_t len = sizeof(value);
    if (getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &value, &len) != 0) {
        return -1;
    }
    return value;
}

static int unix_sockaddr_path(const struct sockaddr *addr, socklen_t addrlen, char *path, size_t path_len, int *abstract) {
    if (addr == NULL || addrlen < offsetof(struct sockaddr_un, sun_path) || path == NULL || path_len == 0) {
        return -1;
    }
    const struct sockaddr_un *un = (const struct sockaddr_un *)addr;
    if (un->sun_family != AF_UNIX) {
        return -1;
    }
    size_t base = offsetof(struct sockaddr_un, sun_path);
    size_t raw_len = addrlen > base ? (size_t)(addrlen - base) : 0;
    if (raw_len == 0) {
        return -1;
    }
    if (un->sun_path[0] == '\0') {
        if (abstract != NULL) {
            *abstract = 1;
        }
        return -1;
    }
    if (abstract != NULL) {
        *abstract = 0;
    }
    size_t n = strnlen(un->sun_path, raw_len);
    if (n == 0 || n >= path_len) {
        return -1;
    }
    memcpy(path, un->sun_path, n);
    path[n] = '\0';
    return 0;
}

static int path_under_root(const char *path, const char *root) {
    if (path == NULL || root == NULL || path[0] == '\0' || root[0] == '\0') {
        return 0;
    }
    size_t root_len = strlen(root);
    if (strncmp(path, root, root_len) != 0) {
        return 0;
    }
    return path[root_len] == '\0' || path[root_len] == '/';
}

static int broker_path_for_unix_socket(const char *path, char *out, size_t out_len) {
    if (path == NULL || path[0] != '/' || out == NULL || out_len == 0) {
        return -1;
    }
    const char *host_root = getenv("OCTOPOS_HOST_CLUSTER_ROOT");
    if (host_root == NULL || host_root[0] == '\0') {
        host_root = "/cluster";
    }
    if (path_under_root(path, host_root)) {
        if (snprintf(out, out_len, "%s", path) >= (int)out_len) {
            return -1;
        }
        return 0;
    }
    if (snprintf(out, out_len, "%s%s", host_root, path) >= (int)out_len) {
        return -1;
    }
    return 0;
}

static void write_errno_status(int fd, int value) {
    ssize_t written = write(fd, &value, sizeof(value));
    (void)written;
}

static int spawn_unixsock_proxy(int app_fd, const char *path) {
    int (*real_socketpair)(int, int, int, int[2]) = dlsym(RTLD_NEXT, "socketpair");
    if (real_socketpair == NULL) {
        errno = ENOSYS;
        return -1;
    }

    int pair[2] = {-1, -1};
    if (real_socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) {
        return -1;
    }

    int exec_pipe[2] = {-1, -1};
    if (pipe(exec_pipe) != 0) {
        int saved = errno;
        close(pair[0]);
        close(pair[1]);
        errno = saved;
        return -1;
    }
    (void)fcntl(exec_pipe[1], F_SETFD, FD_CLOEXEC);

    pid_t pid = fork();
    if (pid < 0) {
        int saved = errno;
        close(pair[0]);
        close(pair[1]);
        close(exec_pipe[0]);
        close(exec_pipe[1]);
        errno = saved;
        return -1;
    }
    if (pid == 0) {
        close(exec_pipe[0]);
        pid_t grandchild = fork();
        if (grandchild < 0) {
            int saved = errno;
            write_errno_status(exec_pipe[1], saved);
            _exit(127);
        }
        if (grandchild > 0) {
            _exit(0);
        }

        close(pair[0]);
        if (dup2(pair[1], STDIN_FILENO) < 0 || dup2(pair[1], STDOUT_FILENO) < 0) {
            int saved = errno;
            write_errno_status(exec_pipe[1], saved);
            _exit(127);
        }
        if (pair[1] != STDIN_FILENO && pair[1] != STDOUT_FILENO) {
            close(pair[1]);
        }
        setenv("OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE", "1", 1);
        execlp(unixsock_proxy_binary(), unixsock_proxy_binary(),
               "--addr", broker_addr(),
               "--stdio",
               "--path", path,
               (char *)NULL);
        int saved = errno;
        write_errno_status(exec_pipe[1], saved);
        _exit(127);
    }

    close(exec_pipe[1]);
    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            break;
        }
    }
    int exec_errno = 0;
    ssize_t exec_status = read(exec_pipe[0], &exec_errno, sizeof(exec_errno));
    close(exec_pipe[0]);
    if (exec_status > 0) {
        close(pair[0]);
        close(pair[1]);
        errno = exec_errno != 0 ? exec_errno : ECHILD;
        return -1;
    }

    close(pair[1]);
    if (dup2(pair[0], app_fd) < 0) {
        int saved = errno;
        close(pair[0]);
        errno = saved;
        return -1;
    }
    close(pair[0]);
    return 0;
}

static int read_errno_status(int fd, int *value) {
    int out = 0;
    ssize_t n = read(fd, &out, sizeof(out));
    if (n == (ssize_t)sizeof(out)) {
        *value = out;
        return 0;
    }
    if (n == 0) {
        *value = ECHILD;
        return -1;
    }
    *value = errno != 0 ? errno : EIO;
    return -1;
}

static int spawn_fifo_proxy(const char *path, const char *mode, int flags) {
    int (*real_socketpair)(int, int, int, int[2]) = dlsym(RTLD_NEXT, "socketpair");
    if (real_socketpair == NULL) {
        errno = ENOSYS;
        return -1;
    }

    int pair[2] = {-1, -1};
    if (real_socketpair(AF_UNIX, SOCK_STREAM, 0, pair) != 0) {
        return -1;
    }

    int ready_pipe[2] = {-1, -1};
    if (pipe(ready_pipe) != 0) {
        int saved = errno;
        close(pair[0]);
        close(pair[1]);
        errno = saved;
        return -1;
    }

    pid_t pid = fork();
    if (pid < 0) {
        int saved = errno;
        close(pair[0]);
        close(pair[1]);
        close(ready_pipe[0]);
        close(ready_pipe[1]);
        errno = saved;
        return -1;
    }
    if (pid == 0) {
        close(ready_pipe[0]);
        pid_t grandchild = fork();
        if (grandchild < 0) {
            int saved = errno;
            write_errno_status(ready_pipe[1], saved);
            _exit(127);
        }
        if (grandchild > 0) {
            _exit(0);
        }

        close(pair[0]);
        if (dup2(pair[1], STDIN_FILENO) < 0 || dup2(pair[1], STDOUT_FILENO) < 0) {
            int saved = errno;
            write_errno_status(ready_pipe[1], saved);
            _exit(127);
        }
        if (pair[1] != STDIN_FILENO && pair[1] != STDOUT_FILENO) {
            close(pair[1]);
        }
        if (ready_pipe[1] != 3 && dup2(ready_pipe[1], 3) < 0) {
            int saved = errno;
            write_errno_status(ready_pipe[1], saved);
            _exit(127);
        }
        if (ready_pipe[1] != 3) {
            close(ready_pipe[1]);
        }
        setenv("OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE", "1", 1);
        execlp(fifo_proxy_binary(), fifo_proxy_binary(),
               "--addr", broker_addr(),
               "--mode", mode,
               "--path", path,
               "--ready-fd", "3",
               (char *)NULL);
        int saved = errno;
        write_errno_status(3, saved);
        _exit(127);
    }

    close(pair[1]);
    close(ready_pipe[1]);
    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            break;
        }
    }
    int ready_errno = 0;
    int ready_status = read_errno_status(ready_pipe[0], &ready_errno);
    close(ready_pipe[0]);
    if (ready_status != 0 || ready_errno != 0) {
        close(pair[0]);
        errno = ready_errno != 0 ? ready_errno : EIO;
        return -1;
    }
    if ((flags & O_CLOEXEC) != 0) {
        (void)fcntl(pair[0], F_SETFD, FD_CLOEXEC);
    }
    return pair[0];
}

struct unixsock_binding {
    int fd;
    int registered;
    char path[4096];
    struct unixsock_binding *next;
};

static struct unixsock_binding *unixsock_bindings = NULL;

static struct unixsock_binding *find_unixsock_binding(int fd) {
    for (struct unixsock_binding *cur = unixsock_bindings; cur != NULL; cur = cur->next) {
        if (cur->fd == fd) {
            return cur;
        }
    }
    return NULL;
}

static int remember_unixsock_binding(int fd, const char *path) {
    if (path == NULL || path[0] == '\0') {
        return -1;
    }
    struct unixsock_binding *binding = find_unixsock_binding(fd);
    if (binding == NULL) {
        binding = calloc(1, sizeof(*binding));
        if (binding == NULL) {
            return -1;
        }
        binding->fd = fd;
        binding->next = unixsock_bindings;
        unixsock_bindings = binding;
    }
    if (snprintf(binding->path, sizeof(binding->path), "%s", path) >= (int)sizeof(binding->path)) {
        return -1;
    }
    binding->registered = 0;
    return 0;
}

static void forget_unixsock_binding(int fd) {
    struct unixsock_binding **cur = &unixsock_bindings;
    while (*cur != NULL) {
        if ((*cur)->fd == fd) {
            struct unixsock_binding *victim = *cur;
            *cur = victim->next;
            free(victim);
            return;
        }
        cur = &(*cur)->next;
    }
}

static int run_unixsock_broker_command(int do_register, const char *path) {
    if (path == NULL || path[0] == '\0') {
        errno = EINVAL;
        return -1;
    }
    pid_t pid = fork();
    if (pid < 0) {
        return -1;
    }
    if (pid == 0) {
        setenv("OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE", "1", 1);
        if (do_register) {
            execlp(unixsock_proxy_binary(), unixsock_proxy_binary(),
                   "--addr", broker_addr(),
                   "--register",
                   "--path", path,
                   "--target", path,
                   (char *)NULL);
        } else {
            execlp(unixsock_proxy_binary(), unixsock_proxy_binary(),
                   "--addr", broker_addr(),
                   "--unregister",
                   "--path", path,
                   (char *)NULL);
        }
        _exit(127);
    }

    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            return -1;
        }
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        errno = ECONNREFUSED;
        return -1;
    }
    return 0;
}

static int unregister_unixsock_binding(int fd) {
    struct unixsock_binding *binding = find_unixsock_binding(fd);
    if (binding == NULL) {
        return 0;
    }
    int rc = 0;
    if (binding->registered) {
        rc = run_unixsock_broker_command(0, binding->path);
    }
    forget_unixsock_binding(fd);
    return rc;
}

static int contains_scm_rights(const struct msghdr *msg) {
    if (msg == NULL || msg->msg_control == NULL || msg->msg_controllen == 0) {
        return 0;
    }
    for (struct cmsghdr *cmsg = CMSG_FIRSTHDR((struct msghdr *)msg);
         cmsg != NULL;
         cmsg = CMSG_NXTHDR((struct msghdr *)msg, cmsg)) {
        if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
            return 1;
        }
    }
    return 0;
}

static int path_is_fifo_at(int dirfd, const char *path) {
    if (path == NULL || path[0] == '\0') {
        return 0;
    }
    struct stat st;
    if (fstatat(dirfd, path, &st, 0) != 0) {
        return 0;
    }
    return S_ISFIFO(st.st_mode);
}

static int proxy_fifo_open_at(int dirfd, const char *pathname, int flags) {
    if (pathname == NULL) {
        errno = EINVAL;
        return -1;
    }
    if ((flags & O_NONBLOCK) != 0) {
        return unsupported_fifo("nonblocking FIFO opens are not distributed yet");
    }
    if ((flags & O_ACCMODE) == O_RDWR) {
        return unsupported_fifo("O_RDWR FIFO opens are not distributed");
    }
    if (pathname[0] != '/') {
        return unsupported_fifo("relative FIFO paths are not distributed after launch");
    }

    const char *mode = NULL;
    switch (flags & O_ACCMODE) {
    case O_RDONLY:
        mode = "read";
        break;
    case O_WRONLY:
        mode = "write";
        break;
    default:
        return unsupported_fifo("unsupported FIFO open mode");
    }

    char broker_path[4096];
    if (broker_path_for_unix_socket(pathname, broker_path, sizeof(broker_path)) != 0) {
        return unsupported_fifo("FIFO path is outside the SSI root");
    }
    (void)dirfd;
    return spawn_fifo_proxy(broker_path, mode, flags);
}

static int apply_mmap_policy(int prot, int *flags, int fd) {
    if (!remote_child_process() || !shared_mapping(*flags)) {
        return 0;
    }

    const char *compat = getenv(ipc_compat_env);
    if (compat == NULL || compat[0] == '\0') {
        return 0;
    }

    if (strcmp(compat, "relaxed") == 0 &&
        (prot & PROT_WRITE) == 0 &&
        (*flags & MAP_ANONYMOUS) == 0 &&
        fd_is_relaxed_mmap_file(fd)) {
        *flags = private_mapping_flags(*flags);
        warn_once(ipc_compat_warned_env,
                  "octopos-remote-child: relaxed IPC compatibility converted read-only MAP_SHARED mapping to MAP_PRIVATE",
                  NULL);
        return 0;
    }

    if (strcmp(compat, "strict") == 0 || strcmp(compat, "relaxed") == 0) {
        warn_once(ipc_compat_block_warned_env,
                  "octopos-remote-child: unsupported MAP_SHARED mapping blocked in remote child",
                  blocked_mmap_reason(prot, *flags, fd));
        errno = ENOTSUP;
        return -1;
    }
    return 0;
}

static int should_wrap_path(const char *path, char *const envp[]) {
    if (!remoting_enabled(envp) || path == NULL || path[0] == '\0') {
        return 0;
    }
    const char *helper = remote_child_binary();
    if (strcmp(path, helper) == 0 || strcmp(path, remote_child_path) == 0 || strcmp(path, "octopos-remote-child") == 0) {
        return 0;
    }
    if (has_prefix(path, "/proc/") || has_prefix(path, "/dev/fd/")) {
        return 0;
    }
    return 1;
}

static size_t vector_len(char *const v[]) {
    if (v == NULL) {
        return 0;
    }
    size_t n = 0;
    while (v[n] != NULL) {
        n++;
    }
    return n;
}

static char **build_child_argv(const char *path, char *const argv[]) {
    size_t argc = vector_len(argv);
    char **out = calloc(argc + 4, sizeof(char *));
    if (out == NULL) {
        return NULL;
    }
    out[0] = (char *)remote_child_binary();
    out[1] = "--";
    out[2] = (char *)path;
    for (size_t i = 1; i < argc; i++) {
        out[i + 2] = argv[i];
    }
    out[argc + 2] = NULL;
    return out;
}

static char **build_child_env(char *const envp[]) {
    char **env = envp != NULL ? (char **)envp : environ;
    size_t envc = vector_len(env);
    char **out = calloc(envc + 2, sizeof(char *));
    if (out == NULL) {
        return NULL;
    }
    for (size_t i = 0; i < envc; i++) {
        out[i] = env[i];
    }
    out[envc] = (char *)active_env;
    out[envc + 1] = NULL;
    return out;
}

typedef int (*execve_fn)(const char *, char *const [], char *const []);
typedef int (*execveat_fn)(int, const char *, char *const [], char *const [], int);
typedef int (*execv_fn)(const char *, char *const []);
typedef int (*execvp_fn)(const char *, char *const []);
typedef int (*execvpe_fn)(const char *, char *const [], char *const []);
typedef void *(*mmap_fn)(void *, size_t, int, int, int, off_t);
typedef void *(*mmap64_fn)(void *, size_t, int, int, int, off64_t);
typedef int (*posix_spawn_fn)(pid_t *, const char *, const posix_spawn_file_actions_t *, const posix_spawnattr_t *, char *const [], char *const []);
typedef int (*system_fn)(const char *);
typedef int (*socket_fn)(int, int, int);
typedef int (*socketpair_fn)(int, int, int, int[2]);
typedef int (*connect_fn)(int, const struct sockaddr *, socklen_t);
typedef int (*bind_fn)(int, const struct sockaddr *, socklen_t);
typedef int (*listen_fn)(int, int);
typedef ssize_t (*sendmsg_fn)(int, const struct msghdr *, int);
typedef ssize_t (*recvmsg_fn)(int, struct msghdr *, int);
typedef int (*getsockopt_fn)(int, int, int, void *, socklen_t *);
typedef int (*setsockopt_fn)(int, int, int, const void *, socklen_t);
typedef int (*close_fn)(int);
typedef int (*open_fn)(const char *, int, ...);
typedef int (*openat_fn)(int, const char *, int, ...);
typedef int (*mkfifo_fn)(const char *, mode_t);
typedef int (*mkfifoat_fn)(int, const char *, mode_t);
typedef int (*eventfd_fn)(unsigned int, int);
typedef int (*eventfd2_fn)(unsigned int, int);
typedef int (*signalfd_fn)(int, const sigset_t *, int);
typedef int (*timerfd_create_fn)(int, int);
typedef int (*memfd_create_fn)(const char *, unsigned int);
typedef int (*shm_open_fn)(const char *, int, mode_t);
typedef int (*shmget_fn)(key_t, size_t, int);
typedef void *(*shmat_fn)(int, const void *, int);
typedef int (*semget_fn)(key_t, int, int);
typedef int (*msgget_fn)(key_t, int);
typedef int (*inotify_init_fn)(void);
typedef int (*inotify_init1_fn)(int);
typedef int (*fanotify_init_fn)(unsigned int, unsigned int);
typedef long (*ptrace_fn)(enum __ptrace_request, ...);

int execve(const char *pathname, char *const argv[], char *const envp[]) {
    execve_fn real_execve = (execve_fn)dlsym(RTLD_NEXT, "execve");
    if (real_execve == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (!should_wrap_path(pathname, envp)) {
        return real_execve(pathname, argv, envp);
    }
    char **child_argv = build_child_argv(pathname, argv);
    char **child_env = build_child_env(envp);
    if (child_argv == NULL || child_env == NULL) {
        free(child_argv);
        free(child_env);
        errno = ENOMEM;
        return -1;
    }
    int rc = real_execve(remote_child_binary(), child_argv, child_env);
    int saved = errno;
    free(child_argv);
    free(child_env);
    errno = saved;
    return rc;
}

int __execve(const char *pathname, char *const argv[], char *const envp[]) {
    return execve(pathname, argv, envp);
}

int execveat(int dirfd, const char *pathname, char *const argv[], char *const envp[], int flags) {
    execveat_fn real_execveat = (execveat_fn)dlsym(RTLD_NEXT, "execveat");
    if (real_execveat == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (dirfd != AT_FDCWD || (flags & AT_EMPTY_PATH) != 0 || !should_wrap_path(pathname, envp)) {
        return real_execveat(dirfd, pathname, argv, envp, flags);
    }
    return execve(pathname, argv, envp);
}

int __execveat(int dirfd, const char *pathname, char *const argv[], char *const envp[], int flags) {
    return execveat(dirfd, pathname, argv, envp, flags);
}

int execv(const char *path, char *const argv[]) {
    execv_fn real_execv = (execv_fn)dlsym(RTLD_NEXT, "execv");
    if (real_execv == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (!should_wrap_path(path, environ)) {
        return real_execv(path, argv);
    }
    return execve(path, argv, environ);
}

int execvp(const char *file, char *const argv[]) {
    execvp_fn real_execvp = (execvp_fn)dlsym(RTLD_NEXT, "execvp");
    if (real_execvp == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (!should_wrap_path(file, environ)) {
        return real_execvp(file, argv);
    }
    return execve(file, argv, environ);
}

int execvpe(const char *file, char *const argv[], char *const envp[]) {
    execvpe_fn real_execvpe = (execvpe_fn)dlsym(RTLD_NEXT, "execvpe");
    if (real_execvpe == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (!should_wrap_path(file, envp)) {
        return real_execvpe(file, argv, envp);
    }
    return execve(file, argv, envp);
}

int posix_spawn(pid_t *pid, const char *path, const posix_spawn_file_actions_t *actions, const posix_spawnattr_t *attrp, char *const argv[], char *const envp[]) {
    posix_spawn_fn real_spawn = (posix_spawn_fn)dlsym(RTLD_NEXT, "posix_spawn");
    if (real_spawn == NULL) {
        return ENOSYS;
    }
    if (!should_wrap_path(path, envp)) {
        return real_spawn(pid, path, actions, attrp, argv, envp);
    }
    char **child_argv = build_child_argv(path, argv);
    char **child_env = build_child_env(envp);
    if (child_argv == NULL || child_env == NULL) {
        free(child_argv);
        free(child_env);
        return ENOMEM;
    }
    int rc = real_spawn(pid, remote_child_binary(), actions, attrp, child_argv, child_env);
    free(child_argv);
    free(child_env);
    return rc;
}

int posix_spawnp(pid_t *pid, const char *file, const posix_spawn_file_actions_t *actions, const posix_spawnattr_t *attrp, char *const argv[], char *const envp[]) {
    posix_spawn_fn real_spawnp = (posix_spawn_fn)dlsym(RTLD_NEXT, "posix_spawnp");
    if (real_spawnp == NULL) {
        return ENOSYS;
    }
    if (!should_wrap_path(file, envp)) {
        return real_spawnp(pid, file, actions, attrp, argv, envp);
    }
    char **child_argv = build_child_argv(file, argv);
    char **child_env = build_child_env(envp);
    if (child_argv == NULL || child_env == NULL) {
        free(child_argv);
        free(child_env);
        return ENOMEM;
    }
    int rc = real_spawnp(pid, remote_child_binary(), actions, attrp, child_argv, child_env);
    free(child_argv);
    free(child_env);
    return rc;
}

int system(const char *command) {
    system_fn real_system = (system_fn)dlsym(RTLD_NEXT, "system");
    if (real_system == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (command == NULL || !remoting_enabled(environ)) {
        return real_system(command);
    }

    pid_t pid = fork();
    if (pid < 0) {
        return -1;
    }
    if (pid == 0) {
        char *shell_argv[] = {"sh", "-c", (char *)command, NULL};
        char **child_argv = build_child_argv("/bin/sh", shell_argv);
        char **child_env = build_child_env(environ);
        execve_fn real_execve = (execve_fn)dlsym(RTLD_NEXT, "execve");
        if (child_argv != NULL && child_env != NULL && real_execve != NULL) {
            real_execve(remote_child_binary(), child_argv, child_env);
        }
        _exit(127);
    }

    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            return -1;
        }
    }
    return status;
}

void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset) {
    mmap_fn real_mmap = (mmap_fn)dlsym(RTLD_NEXT, "mmap");
    if (real_mmap == NULL) {
        errno = ENOSYS;
        return MAP_FAILED;
    }
    int adjusted_flags = flags;
    if (apply_mmap_policy(prot, &adjusted_flags, fd) != 0) {
        return MAP_FAILED;
    }
    return real_mmap(addr, length, prot, adjusted_flags, fd, offset);
}

void *mmap64(void *addr, size_t length, int prot, int flags, int fd, off64_t offset) {
    mmap64_fn real_mmap64 = (mmap64_fn)dlsym(RTLD_NEXT, "mmap64");
    if (real_mmap64 == NULL) {
        errno = ENOSYS;
        return MAP_FAILED;
    }
    int adjusted_flags = flags;
    if (apply_mmap_policy(prot, &adjusted_flags, fd) != 0) {
        return MAP_FAILED;
    }
    return real_mmap64(addr, length, prot, adjusted_flags, fd, offset);
}

int socket(int domain, int type, int protocol) {
    socket_fn real_socket = (socket_fn)dlsym(RTLD_NEXT, "socket");
    if (real_socket == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process() && domain == AF_NETLINK) {
        return unsupported_kernel_ipc("netlink sockets refer to node-local kernel state");
    }
    if (remote_child_process() && domain == AF_UNIX && socket_type_value(type) != SOCK_STREAM) {
        return unsupported_unix_socket("only AF_UNIX/SOCK_STREAM is brokerable");
    }
    return real_socket(domain, type, protocol);
}

int socketpair(int domain, int type, int protocol, int sv[2]) {
    socketpair_fn real_socketpair = (socketpair_fn)dlsym(RTLD_NEXT, "socketpair");
    if (real_socketpair == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process() && domain == AF_UNIX) {
        return unsupported_unix_socket("socketpair(AF_UNIX) requires local kernel peer state");
    }
    return real_socketpair(domain, type, protocol, sv);
}

int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
    connect_fn real_connect = (connect_fn)dlsym(RTLD_NEXT, "connect");
    if (real_connect == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (!remote_child_process() || addr == NULL || addr->sa_family != AF_UNIX) {
        return real_connect(sockfd, addr, addrlen);
    }
    if (fd_socket_type(sockfd) != SOCK_STREAM) {
        return unsupported_unix_socket("only AF_UNIX/SOCK_STREAM connect is brokerable");
    }

    char path[sizeof(((struct sockaddr_un *)0)->sun_path) + 1];
    int abstract = 0;
    if (unix_sockaddr_path(addr, addrlen, path, sizeof(path), &abstract) != 0) {
        if (abstract) {
            return unsupported_unix_socket("abstract Unix sockets are not distributed");
        }
        return unsupported_unix_socket("Unix socket connect requires an absolute filesystem path");
    }
    if (path[0] != '/') {
        return unsupported_unix_socket("relative Unix socket paths are not distributed");
    }

    char broker_path[4096];
    if (broker_path_for_unix_socket(path, broker_path, sizeof(broker_path)) != 0) {
        return unsupported_unix_socket("Unix socket path is outside the SSI root");
    }
    if (spawn_unixsock_proxy(sockfd, broker_path) != 0) {
        return -1;
    }
    return 0;
}

int bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
    bind_fn real_bind = (bind_fn)dlsym(RTLD_NEXT, "bind");
    if (real_bind == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (!remote_child_process() || addr == NULL || addr->sa_family != AF_UNIX) {
        return real_bind(sockfd, addr, addrlen);
    }
    if (fd_socket_type(sockfd) != SOCK_STREAM) {
        return unsupported_unix_socket("only AF_UNIX/SOCK_STREAM bind is brokerable");
    }

    char path[sizeof(((struct sockaddr_un *)0)->sun_path) + 1];
    int abstract = 0;
    if (unix_sockaddr_path(addr, addrlen, path, sizeof(path), &abstract) != 0) {
        if (abstract) {
            return unsupported_unix_socket("abstract Unix sockets are not distributed");
        }
        return unsupported_unix_socket("Unix socket bind requires an absolute filesystem path");
    }
    if (path[0] != '/') {
        return unsupported_unix_socket("relative Unix socket paths are not distributed");
    }

    char broker_path[4096];
    if (broker_path_for_unix_socket(path, broker_path, sizeof(broker_path)) != 0) {
        return unsupported_unix_socket("Unix socket path is outside the SSI root");
    }
    if (real_bind(sockfd, addr, addrlen) != 0) {
        return -1;
    }
    if (remember_unixsock_binding(sockfd, broker_path) != 0) {
        errno = ENOMEM;
        return -1;
    }
    return 0;
}

int listen(int sockfd, int backlog) {
    listen_fn real_listen = (listen_fn)dlsym(RTLD_NEXT, "listen");
    if (real_listen == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (!remote_child_process() || fd_socket_domain(sockfd) != AF_UNIX) {
        return real_listen(sockfd, backlog);
    }
    struct unixsock_binding *binding = find_unixsock_binding(sockfd);
    if (binding == NULL) {
        return unsupported_unix_socket("AF_UNIX listen requires a brokerable pathname bind first");
    }
    if (real_listen(sockfd, backlog) != 0) {
        return -1;
    }
    if (!binding->registered) {
        if (run_unixsock_broker_command(1, binding->path) != 0) {
            return -1;
        }
        binding->registered = 1;
    }
    return 0;
}

int close(int fd) {
    close_fn real_close = (close_fn)dlsym(RTLD_NEXT, "close");
    if (real_close == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        (void)unregister_unixsock_binding(fd);
    }
    return real_close(fd);
}

ssize_t sendmsg(int sockfd, const struct msghdr *msg, int flags) {
    sendmsg_fn real_sendmsg = (sendmsg_fn)dlsym(RTLD_NEXT, "sendmsg");
    if (real_sendmsg == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process() && contains_scm_rights(msg)) {
        return unsupported_unix_socket("SCM_RIGHTS file descriptor passing is not distributed");
    }
    return real_sendmsg(sockfd, msg, flags);
}

ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags) {
    recvmsg_fn real_recvmsg = (recvmsg_fn)dlsym(RTLD_NEXT, "recvmsg");
    if (real_recvmsg == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process() && msg != NULL && msg->msg_control != NULL && msg->msg_controllen > 0) {
        return unsupported_unix_socket("ancillary Unix socket data is not distributed");
    }
    return real_recvmsg(sockfd, msg, flags);
}

int getsockopt(int sockfd, int level, int optname, void *optval, socklen_t *optlen) {
    getsockopt_fn real_getsockopt = (getsockopt_fn)dlsym(RTLD_NEXT, "getsockopt");
    if (real_getsockopt == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process() && level == SOL_SOCKET && optname == SO_PEERCRED) {
        return unsupported_unix_socket("SO_PEERCRED is not meaningful across the Unix socket broker");
    }
    return real_getsockopt(sockfd, level, optname, optval, optlen);
}

int setsockopt(int sockfd, int level, int optname, const void *optval, socklen_t optlen) {
    setsockopt_fn real_setsockopt = (setsockopt_fn)dlsym(RTLD_NEXT, "setsockopt");
    if (real_setsockopt == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process() && level == SOL_SOCKET && optname == SO_PASSCRED) {
        return unsupported_unix_socket("SO_PASSCRED is not meaningful across the Unix socket broker");
    }
    return real_setsockopt(sockfd, level, optname, optval, optlen);
}

int open(const char *pathname, int flags, ...) {
    open_fn real_open = (open_fn)dlsym(RTLD_NEXT, "open");
    if (real_open == NULL) {
        errno = ENOSYS;
        return -1;
    }
    mode_t mode = 0;
    if ((flags & O_CREAT) != 0) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    if (fifo_proxy_enabled() && path_is_fifo_at(AT_FDCWD, pathname)) {
        return proxy_fifo_open_at(AT_FDCWD, pathname, flags);
    }
    if ((flags & O_CREAT) != 0) {
        return real_open(pathname, flags, mode);
    }
    return real_open(pathname, flags);
}

int open64(const char *pathname, int flags, ...) {
    open_fn real_open64 = (open_fn)dlsym(RTLD_NEXT, "open64");
    if (real_open64 == NULL) {
        errno = ENOSYS;
        return -1;
    }
    mode_t mode = 0;
    if ((flags & O_CREAT) != 0) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    if (fifo_proxy_enabled() && path_is_fifo_at(AT_FDCWD, pathname)) {
        return proxy_fifo_open_at(AT_FDCWD, pathname, flags);
    }
    if ((flags & O_CREAT) != 0) {
        return real_open64(pathname, flags, mode);
    }
    return real_open64(pathname, flags);
}

int openat(int dirfd, const char *pathname, int flags, ...) {
    openat_fn real_openat = (openat_fn)dlsym(RTLD_NEXT, "openat");
    if (real_openat == NULL) {
        errno = ENOSYS;
        return -1;
    }
    mode_t mode = 0;
    if ((flags & O_CREAT) != 0) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    if (fifo_proxy_enabled() && path_is_fifo_at(dirfd, pathname)) {
        return proxy_fifo_open_at(dirfd, pathname, flags);
    }
    if ((flags & O_CREAT) != 0) {
        return real_openat(dirfd, pathname, flags, mode);
    }
    return real_openat(dirfd, pathname, flags);
}

int openat64(int dirfd, const char *pathname, int flags, ...) {
    openat_fn real_openat64 = (openat_fn)dlsym(RTLD_NEXT, "openat64");
    if (real_openat64 == NULL) {
        errno = ENOSYS;
        return -1;
    }
    mode_t mode = 0;
    if ((flags & O_CREAT) != 0) {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    if (fifo_proxy_enabled() && path_is_fifo_at(dirfd, pathname)) {
        return proxy_fifo_open_at(dirfd, pathname, flags);
    }
    if ((flags & O_CREAT) != 0) {
        return real_openat64(dirfd, pathname, flags, mode);
    }
    return real_openat64(dirfd, pathname, flags);
}

int mkfifo(const char *pathname, mode_t mode) {
    mkfifo_fn real_mkfifo = (mkfifo_fn)dlsym(RTLD_NEXT, "mkfifo");
    if (real_mkfifo == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return real_mkfifo(pathname, mode);
}

int mkfifoat(int dirfd, const char *pathname, mode_t mode) {
    mkfifoat_fn real_mkfifoat = (mkfifoat_fn)dlsym(RTLD_NEXT, "mkfifoat");
    if (real_mkfifoat == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return real_mkfifoat(dirfd, pathname, mode);
}

int eventfd(unsigned int initval, int flags) {
    eventfd_fn real_eventfd = (eventfd_fn)dlsym(RTLD_NEXT, "eventfd");
    if (real_eventfd == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("eventfd counters are local kernel state");
    }
    return real_eventfd(initval, flags);
}

int eventfd2(unsigned int initval, int flags) {
    eventfd2_fn real_eventfd2 = (eventfd2_fn)dlsym(RTLD_NEXT, "eventfd2");
    if (real_eventfd2 == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("eventfd counters are local kernel state");
    }
    return real_eventfd2(initval, flags);
}

int signalfd(int fd, const sigset_t *mask, int flags) {
    signalfd_fn real_signalfd = (signalfd_fn)dlsym(RTLD_NEXT, "signalfd");
    if (real_signalfd == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("signalfd depends on node-local signal delivery state");
    }
    return real_signalfd(fd, mask, flags);
}

int timerfd_create(int clockid, int flags) {
    timerfd_create_fn real_timerfd_create = (timerfd_create_fn)dlsym(RTLD_NEXT, "timerfd_create");
    if (real_timerfd_create == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("timerfd state is not distributed");
    }
    return real_timerfd_create(clockid, flags);
}

int memfd_create(const char *name, unsigned int flags) {
    memfd_create_fn real_memfd_create = (memfd_create_fn)dlsym(RTLD_NEXT, "memfd_create");
    if (real_memfd_create == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("memfd shared-memory objects are local kernel state");
    }
    return real_memfd_create(name, flags);
}

int shm_open(const char *name, int oflag, mode_t mode) {
    shm_open_fn real_shm_open = (shm_open_fn)dlsym(RTLD_NEXT, "shm_open");
    if (real_shm_open == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("POSIX shared memory is not distributed");
    }
    return real_shm_open(name, oflag, mode);
}

int shmget(key_t key, size_t size, int shmflg) {
    shmget_fn real_shmget = (shmget_fn)dlsym(RTLD_NEXT, "shmget");
    if (real_shmget == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("SysV shared memory is not distributed");
    }
    return real_shmget(key, size, shmflg);
}

void *shmat(int shmid, const void *shmaddr, int shmflg) {
    shmat_fn real_shmat = (shmat_fn)dlsym(RTLD_NEXT, "shmat");
    if (real_shmat == NULL) {
        errno = ENOSYS;
        return (void *)-1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc_ptr("SysV shared memory attach is not distributed");
    }
    return real_shmat(shmid, shmaddr, shmflg);
}

int semget(key_t key, int nsems, int semflg) {
    semget_fn real_semget = (semget_fn)dlsym(RTLD_NEXT, "semget");
    if (real_semget == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("SysV semaphores are not distributed");
    }
    return real_semget(key, nsems, semflg);
}

int msgget(key_t key, int msgflg) {
    msgget_fn real_msgget = (msgget_fn)dlsym(RTLD_NEXT, "msgget");
    if (real_msgget == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("SysV message queues are not distributed");
    }
    return real_msgget(key, msgflg);
}

int inotify_init(void) {
    inotify_init_fn real_inotify_init = (inotify_init_fn)dlsym(RTLD_NEXT, "inotify_init");
    if (real_inotify_init == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("inotify state is node-local");
    }
    return real_inotify_init();
}

int inotify_init1(int flags) {
    inotify_init1_fn real_inotify_init1 = (inotify_init1_fn)dlsym(RTLD_NEXT, "inotify_init1");
    if (real_inotify_init1 == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("inotify state is node-local");
    }
    return real_inotify_init1(flags);
}

int fanotify_init(unsigned int flags, unsigned int event_f_flags) {
    fanotify_init_fn real_fanotify_init = (fanotify_init_fn)dlsym(RTLD_NEXT, "fanotify_init");
    if (real_fanotify_init == NULL) {
        errno = ENOSYS;
        return -1;
    }
    if (remote_child_process()) {
        return unsupported_kernel_ipc("fanotify state is node-local");
    }
    return real_fanotify_init(flags, event_f_flags);
}

long ptrace(enum __ptrace_request request, ...) {
    ptrace_fn real_ptrace = (ptrace_fn)dlsym(RTLD_NEXT, "ptrace");
    if (real_ptrace == NULL) {
        errno = ENOSYS;
        return -1;
    }
    va_list ap;
    va_start(ap, request);
    pid_t pid = va_arg(ap, pid_t);
    void *addr = va_arg(ap, void *);
    void *data = va_arg(ap, void *);
    va_end(ap);
    if (remote_child_process()) {
        return unsupported_kernel_ipc("ptrace is not distributed");
    }
    return real_ptrace(request, pid, addr, data);
}

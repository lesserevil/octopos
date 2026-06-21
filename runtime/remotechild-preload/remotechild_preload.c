#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <spawn.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

#ifndef AT_EMPTY_PATH
#define AT_EMPTY_PATH 0x1000
#endif

extern char **environ;

static const char *remote_child_path = "/usr/local/bin/octopos-remote-child";
static const char *active_env = "OCTOPOS_REMOTE_CHILD_PRELOAD_ACTIVE=1";
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

static int should_wrap_path(const char *path, char *const envp[]) {
    if (!remoting_enabled(envp) || path == NULL || path[0] == '\0') {
        return 0;
    }
    if (strcmp(path, remote_child_path) == 0 || strcmp(path, "octopos-remote-child") == 0) {
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
    out[0] = (char *)remote_child_path;
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
typedef int (*posix_spawn_fn)(pid_t *, const char *, const posix_spawn_file_actions_t *, const posix_spawnattr_t *, char *const [], char *const []);

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
    int rc = real_execve(remote_child_path, child_argv, child_env);
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
    int rc = real_spawn(pid, remote_child_path, actions, attrp, child_argv, child_env);
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
    int rc = real_spawnp(pid, remote_child_path, actions, attrp, child_argv, child_env);
    free(child_argv);
    free(child_env);
    return rc;
}

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// mobyDefault is the allow list of moby's default seccomp profile
// (github.com/moby/profiles, seccomp/v0.2.4), which most container runtimes
// apply as RuntimeDefault: every syscall it allows without a capability or
// architecture condition, plus ptrace and process_vm_{readv,writev}, which it
// allows on kernels from 4.8. riscv_hwprobe is left out because neither
// architecture below has it.
var mobyDefault = []string{
	"_llseek", "_newselect", "accept", "accept4", "access", "adjtimex", "alarm", "bind", "brk",
	"cachestat", "capget", "capset", "chdir", "chmod", "chown", "chown32", "clock_adjtime",
	"clock_adjtime64", "clock_getres", "clock_getres_time64", "clock_gettime",
	"clock_gettime64", "clock_nanosleep", "clock_nanosleep_time64", "close", "close_range",
	"connect", "copy_file_range", "creat", "dup", "dup2", "dup3", "epoll_create",
	"epoll_create1", "epoll_ctl", "epoll_ctl_old", "epoll_pwait", "epoll_pwait2", "epoll_wait",
	"epoll_wait_old", "eventfd", "eventfd2", "execve", "execveat", "exit", "exit_group",
	"faccessat", "faccessat2", "fadvise64", "fadvise64_64", "fallocate", "fanotify_mark",
	"fchdir", "fchmod", "fchmodat", "fchmodat2", "fchown", "fchown32", "fchownat", "fcntl",
	"fcntl64", "fdatasync", "fgetxattr", "flistxattr", "flock", "fork", "fremovexattr",
	"fsetxattr", "fstat", "fstat64", "fstatat64", "fstatfs", "fstatfs64", "fsync", "ftruncate",
	"ftruncate64", "futex", "futex_requeue", "futex_time64", "futex_wait", "futex_waitv",
	"futex_wake", "futimesat", "get_robust_list", "get_thread_area", "getcpu", "getcwd",
	"getdents", "getdents64", "getegid", "getegid32", "geteuid", "geteuid32", "getgid",
	"getgid32", "getgroups", "getgroups32", "getitimer", "getpeername", "getpgid", "getpgrp",
	"getpid", "getppid", "getpriority", "getrandom", "getresgid", "getresgid32", "getresuid",
	"getresuid32", "getrlimit", "getrusage", "getsid", "getsockname", "getsockopt", "gettid",
	"gettimeofday", "getuid", "getuid32", "getxattr", "getxattrat", "inotify_add_watch",
	"inotify_init", "inotify_init1", "inotify_rm_watch", "io_cancel", "io_destroy",
	"io_getevents", "io_pgetevents", "io_pgetevents_time64", "io_setup", "io_submit", "ioctl",
	"ioprio_get", "ioprio_set", "ipc", "kill", "landlock_add_rule", "landlock_create_ruleset",
	"landlock_restrict_self", "lchown", "lchown32", "lgetxattr", "link", "linkat", "listen",
	"listmount", "listxattr", "listxattrat", "llistxattr", "lremovexattr", "lseek",
	"lsetxattr", "lstat", "lstat64", "madvise", "map_shadow_stack", "membarrier",
	"memfd_create", "memfd_secret", "mincore", "mkdir", "mkdirat", "mknod", "mknodat", "mlock",
	"mlock2", "mlockall", "mmap", "mmap2", "mprotect", "mq_getsetattr", "mq_notify", "mq_open",
	"mq_timedreceive", "mq_timedreceive_time64", "mq_timedsend", "mq_timedsend_time64",
	"mq_unlink", "mremap", "mseal", "msgctl", "msgget", "msgrcv", "msgsnd", "msync", "munlock",
	"munlockall", "munmap", "name_to_handle_at", "nanosleep", "newfstatat", "open", "openat",
	"openat2", "pause", "pidfd_open", "pidfd_send_signal", "pipe", "pipe2", "pkey_alloc",
	"pkey_free", "pkey_mprotect", "poll", "ppoll", "ppoll_time64", "prctl", "pread64",
	"preadv", "preadv2", "prlimit64", "process_mrelease", "pselect6", "pselect6_time64",
	"pwrite64", "pwritev", "pwritev2", "read", "readahead", "readlink", "readlinkat", "readv",
	"recv", "recvfrom", "recvmmsg", "recvmmsg_time64", "recvmsg", "remap_file_pages",
	"removexattr", "removexattrat", "rename", "renameat", "renameat2", "restart_syscall",
	"rmdir", "rseq", "rt_sigaction", "rt_sigpending", "rt_sigprocmask", "rt_sigqueueinfo",
	"rt_sigreturn", "rt_sigsuspend", "rt_sigtimedwait", "rt_sigtimedwait_time64",
	"rt_tgsigqueueinfo", "sched_get_priority_max", "sched_get_priority_min",
	"sched_getaffinity", "sched_getattr", "sched_getparam", "sched_getscheduler",
	"sched_rr_get_interval", "sched_rr_get_interval_time64", "sched_setaffinity",
	"sched_setattr", "sched_setparam", "sched_setscheduler", "sched_yield", "seccomp",
	"select", "semctl", "semget", "semop", "semtimedop", "semtimedop_time64", "send",
	"sendfile", "sendfile64", "sendmmsg", "sendmsg", "sendto", "set_robust_list",
	"set_thread_area", "set_tid_address", "setfsgid", "setfsgid32", "setfsuid", "setfsuid32",
	"setgid", "setgid32", "setgroups", "setgroups32", "setitimer", "setpgid", "setpriority",
	"setregid", "setregid32", "setresgid", "setresgid32", "setresuid", "setresuid32",
	"setreuid", "setreuid32", "setrlimit", "setsid", "setsockopt", "setuid", "setuid32",
	"setxattr", "setxattrat", "shmat", "shmctl", "shmdt", "shmget", "shutdown", "sigaltstack",
	"signalfd", "signalfd4", "sigprocmask", "sigreturn", "socketcall", "socketpair", "splice",
	"stat", "stat64", "statfs", "statfs64", "statmount", "statx", "symlink", "symlinkat",
	"sync", "sync_file_range", "syncfs", "sysinfo", "tee", "tgkill", "time", "timer_create",
	"timer_delete", "timer_getoverrun", "timer_gettime", "timer_gettime64", "timer_settime",
	"timer_settime64", "timerfd_create", "timerfd_gettime", "timerfd_gettime64",
	"timerfd_settime", "timerfd_settime64", "times", "tkill", "truncate", "truncate64",
	"ugetrlimit", "umask", "uname", "unlink", "unlinkat", "uretprobe", "utime", "utimensat",
	"utimensat_time64", "utimes", "vfork", "vmsplice", "wait4", "waitid", "waitpid", "write",
	"writev",
	"process_vm_readv", "process_vm_writev", "ptrace",
}

// archSyscalls are the syscalls moby allows only on some architectures, less
// sync_file_range2, which neither arm architecture has. modify_ldt, which moby
// also allows on amd64, is left out: the worker does not use it.
var archSyscalls = map[string][]string{
	"amd64": {"arch_prctl"},
	"arm64": {"arm_fadvise64_64", "arm_sync_file_range", "breakpoint", "cacheflush", "set_tls"},
}

// notOnArch are mobyDefault entries that none of a profile's seccomp
// architectures has a number for in libseccomp's syscall table, so they are
// left out of that profile.
var notOnArch = map[string][]string{
	"amd64": {"recv", "send"},
	"arm64": {
		"alarm", "epoll_ctl_old", "epoll_wait_old", "fadvise64_64", "get_thread_area", "ipc",
		"select", "set_thread_area", "socketcall", "time", "uretprobe", "utime", "waitpid",
	},
}

// workerSyscalls are the syscalls the worker needs that moby allows only with
// CAP_SYS_ADMIN or CAP_SYS_CHROOT, or (pivot_root) never: runsc, the micro-VM
// worker and virtiofsd create namespaces and mounts for their sandboxes, and
// the worker joins actor network namespaces. The worker holds no
// CAP_SYS_CHROOT, but runsc's gofer calls chroot inside its own user
// namespace, and a seccomp filter applies there too.
var workerSyscalls = []string{
	"chroot", "clone", "clone3", "fsconfig", "fsmount", "fsopen", "mount", "mount_setattr",
	"move_mount", "open_tree", "pivot_root", "setns", "umount2", "unshare",
}

// socketFamilies are the address families socket() accepts, as in moby's
// profile: everything below AF_ALG (38), then AF_KCM (41) through AF_MCTP
// (45), skipping AF_VSOCK (40). AF_NFC (39) is allowed too.
var socketFamilies = struct {
	below uint64
	equal []uint64
}{below: 38, equal: []uint64{39, 41, 42, 43, 44, 45}}

// personalities are the personality() arguments moby allows: PER_LINUX,
// PER_LINUX32, UNAME26, UNAME26|PER_LINUX32 and the 0xffffffff query.
var personalities = []uint64{0x0, 0x8, 0x20000, 0x20008, 0xffffffff}

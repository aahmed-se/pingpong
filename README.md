# pingpong

A small HTTP server in Zig on a hand-written fiber library. One IO thread, a
pool of CPU threads, one allocation, a fixed number of requests in flight.

```sh
zig build run              # serves http://127.0.0.1:8080
zig build run -- 9000      # or another port
zig build test             # fiber library tests
```

Needs Zig 0.17. Developed and tested on macOS (aarch64, and x86_64 under
Rosetta). It also builds for Linux on both architectures, but that has not
been run.

| Route          | Reply                                      | Kind      |
| -------------- | ------------------------------------------ | --------- |
| `GET /ping`    | `pong`                                     | IO-bound  |
| `GET /cpu?n=N` | how many primes are below N (default 10 M) | CPU-bound |

To see the scheduler at work, start a few long computations and ping
alongside them. The computations run in parallel on separate cores, and the
ping comes back as fast as on an idle server.

```sh
curl 'localhost:8080/cpu?n=50000000' &
curl 'localhost:8080/cpu?n=50000000' &
curl localhost:8080/ping
```

## How it works

| File                      | What it does                                                    |
| ------------------------- | --------------------------------------------------------------- |
| `src/fiber/Fiber.zig`     | A fiber: its own stack and an assembly context switch.          |
| `src/fiber/Scheduler.zig` | Fixed pool of fibers, run by an IO thread and a CPU thread pool. |
| `src/net.zig`             | Socket calls that park the calling fiber instead of blocking.   |
| `src/main.zig`            | The server: startup, accept loop, HTTP, the two routes.         |

**Threads.** The main thread is the IO thread: it runs the `poll` loop and
every fiber that is doing socket IO. A fixed pool of CPU threads (one per
core, less one) runs fibers that are computing. Each request runs on its own
fiber and starts on the IO thread. `/cpu` calls `task.moveTo(.cpu)` before
its computation, which suspends the fiber and resumes it on a CPU thread in
the middle of the same function, then `task.moveTo(.io)` to come back and
send the answer. Sockets are only ever touched on the IO thread.

**Priority.** On macOS the CPU threads run at a lower priority than the IO
thread (`config.cpu_thread_priority = .low`). When every core is computing,
that keeps the 99th-percentile ping under a millisecond instead of around
5 ms, but the computations then take roughly half as long again. Set it to
`.normal` for full computing throughput. The setting has no effect on Linux.

**Scheduling.** Scheduling is cooperative. On the IO thread a fiber runs
until it blocks on a socket, and `poll` wakes it when the socket is ready. On
the CPU threads a fiber calls `task.checkpoint()` in its loop, which yields
once its 1 ms time slice is up, so more computations than CPU threads take
turns.

**Handing fibers between threads.** A fiber never queues itself. It records
what it wants, switches out, and the thread that was running it files it
afterwards. Otherwise another thread could resume the fiber while it was
still on its stack. The IO thread owns almost all scheduler state; CPU
threads only touch the CPU queue and an inbox for the IO thread, each behind
a mutex, and wake the IO thread through a pipe.

**Memory.** `main` makes one `mmap` at startup and hands it to the scheduler,
which carves it into the fiber stacks, the CPU threads' stacks (each stack
with a guard page below it) and its own bookkeeping, and touches every page so
the memory is committed up front. Request buffers are locals on the fiber
stacks. Nothing allocates after that, and no thread is created after startup.

**Limits.** With no allocation, capacity is fixed: 64 requests in flight
(`config.max_requests` in `src/main.zig`). Further connections wait in the
kernel's accept queue until a fiber is free. A client gets 5 seconds to send
its request, so idle connections cannot hold fibers forever. One request per
connection; no keep-alive. `config.cpu_threads` overrides the size of the CPU
pool.

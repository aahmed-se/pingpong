//! Scheduler for a fixed pool of fibers, spread over two kinds of thread.
//!
//!   IO thread     The thread that calls `run`. It owns the `poll` loop and
//!                 runs every IO-class fiber. Those run for microseconds
//!                 between waits, so one thread serves them all.
//!
//!   CPU threads   A fixed pool of workers. They take CPU-class fibers from a
//!                 shared queue and run each for one time slice.
//!
//! Every fiber starts in the IO class. It changes class by calling
//! `task.moveTo(.cpu)`: the call suspends the fiber on the IO thread and
//! returns on a CPU thread, in the middle of the same function.
//! `task.moveTo(.io)` brings it back. Computation therefore never delays
//! socket IO, and several computations run in parallel.
//!
//! Handing a fiber to another thread
//!
//! A fiber never puts itself on a queue. It records what it wants in
//! `Task.request` and switches out; the thread that was running it then files
//! it, after the switch has completed. If the fiber queued itself first,
//! another thread could pick it up and resume it while it was still executing
//! on its own stack.
//!
//! Who owns what
//!
//! The IO thread owns nearly everything: the free list, the IO run queue, the
//! poll set and each task's `state`. CPU threads touch only the CPU queue and
//! the inbox, each behind its own mutex. Anything else a CPU thread needs done
//! (releasing a finished fiber, resuming one on the IO thread) it does by
//! putting the task in the inbox and waking the IO thread through a pipe.
//!
//! All memory, including the CPU threads' stacks, comes from the single block
//! passed to `init`. Nothing here allocates afterwards, so the number of
//! fibers and threads is fixed for the life of the process.

const std = @import("std");
const builtin = @import("builtin");
const c = std.c;
const posix = std.posix;
const Fiber = @import("Fiber.zig");

const Scheduler = @This();

/// How long a fiber runs before `checkpoint` makes it yield to others of its
/// class.
pub const time_slice_ns = 1 * std.time.ns_per_ms;

/// Pass as a deadline to wait forever.
pub const no_deadline = std.math.maxInt(u64);

pub const Entry = *const fn (task: *Task, arg: usize) void;

/// Which kind of thread runs a fiber.
pub const Class = enum { io, cpu };

pub const Options = struct {
    /// Size of the fiber pool: the most fibers that can be alive at once.
    fiber_count: usize,
    fiber_stack_size: usize,
    /// Worker threads for CPU-class fibers. At least one.
    cpu_threads: usize,
    cpu_thread_priority: Priority = .normal,
};

/// Scheduling priority of the CPU threads relative to the IO thread.
pub const Priority = enum {
    /// Same as the IO thread. Computation gets the most out of the machine,
    /// but once every core is busy the IO thread has to queue for one like
    /// any other thread, and an occasional IO-bound request waits several
    /// milliseconds.
    normal,
    /// Below the IO thread, so the operating system lets IO (and anything
    /// else on the machine) preempt computation. IO stays quick however busy
    /// the cores are; computation gives up some throughput under full load.
    /// Implemented on macOS only; elsewhere this is the same as `normal`.
    low,
};

/// Stack for each CPU thread. A worker only runs its scheduling loop there
/// (fibers bring their own stacks), but a panic needs room to print a trace.
const thread_stack_size = 256 * 1024;

tasks: []Task,
threads: []c.pthread_t,
/// Scratch space for `poll`: the wake pipe plus one slot per fiber.
pollfds: []posix.pollfd,

// Owned by the IO thread.
free: Queue = .{},
io_ready: Queue = .{},
/// Fibers that have been spawned and have not finished.
live: usize = 0,
/// Fibers parked on a file descriptor.
waiting_io: usize = 0,
/// A fiber parked in `waitForFreeFiber`.
fiber_waiter: ?*Task = null,

// Shared with the CPU threads.
cpu_queue: CpuQueue = .{},
inbox: Inbox = .{},
/// Written by a CPU thread to get the IO thread out of `poll`.
wake_read: posix.fd_t,
wake_write: posix.fd_t,

/// A fiber together with its scheduling state. Each fiber's entry function is
/// given its own task, and uses it to yield, wait and change threads.
pub const Task = struct {
    sched: *Scheduler,
    fiber: Fiber,
    stack: []u8,
    entry: Entry,
    arg: usize,
    /// Links the task into whichever queue currently holds it.
    next: ?*Task,

    class: Class,
    /// What the fiber wanted when it last switched out.
    request: Request,
    /// When the fiber was last switched in.
    slice_start_ns: u64,

    /// Read and written by the IO thread only, even while a CPU thread is
    /// running the fiber.
    state: State,
    // What a `.waiting_io` task is waiting for, and how the wait ended.
    fd: posix.fd_t,
    events: i16,
    deadline_ns: u64,
    timed_out: bool,

    /// Lets other fibers of the same class run, then continues.
    pub fn yield(task: *Task) void {
        task.request = .reschedule;
        task.fiber.switchOut();
    }

    /// Preemption point for long computations: yields only if the fiber has
    /// used up its time slice. Cheap enough to call every few hundred
    /// iterations of a hot loop.
    pub fn checkpoint(task: *Task) void {
        if (now() - task.slice_start_ns >= time_slice_ns) task.yield();
    }

    /// Moves the calling fiber to the other kind of thread. When this returns,
    /// the fiber is running on the IO thread or on one of the CPU threads, as
    /// asked.
    pub fn moveTo(task: *Task, class: Class) void {
        if (task.class == class) return;
        task.class = class;
        task.yield();
    }

    /// Parks the calling fiber until `fd` is readable (or has hung up or
    /// failed, so that the next read reports it), or until `deadline_ns`
    /// passes. The fiber resumes on the IO thread, whichever thread it was on.
    pub fn waitReadable(task: *Task, fd: posix.fd_t, deadline_ns: u64) error{Timeout}!void {
        return task.waitIo(fd, posix.POLL.IN, deadline_ns);
    }

    /// Like `waitReadable`, but waits until `fd` is writable.
    pub fn waitWritable(task: *Task, fd: posix.fd_t, deadline_ns: u64) error{Timeout}!void {
        return task.waitIo(fd, posix.POLL.OUT, deadline_ns);
    }

    fn waitIo(task: *Task, fd: posix.fd_t, events: i16, deadline_ns: u64) error{Timeout}!void {
        task.class = .io;
        task.fd = fd;
        task.events = events;
        task.deadline_ns = deadline_ns;
        task.request = .wait_io;
        task.fiber.switchOut();
        if (task.timed_out) return error.Timeout;
    }

    /// Parks the calling fiber until `Scheduler.spawn` is certain to succeed.
    /// IO-class fibers only, and only one may wait this way at a time.
    pub fn waitForFreeFiber(task: *Task) void {
        std.debug.assert(task.class == .io);
        while (task.sched.free.isEmpty()) {
            task.request = .wait_fiber;
            task.fiber.switchOut();
        }
    }
};

const Request = enum { reschedule, wait_io, wait_fiber };

const State = enum { free, ready, running, waiting_io, waiting_fiber, on_cpu_thread };

/// Intrusive FIFO of tasks, linked through `Task.next`.
const Queue = struct {
    head: ?*Task = null,
    tail: ?*Task = null,

    fn push(q: *Queue, task: *Task) void {
        task.next = null;
        if (q.tail) |tail| tail.next = task else q.head = task;
        q.tail = task;
    }

    fn pop(q: *Queue) ?*Task {
        const task = q.head orelse return null;
        q.head = task.next;
        if (q.head == null) q.tail = null;
        return task;
    }

    fn isEmpty(q: *const Queue) bool {
        return q.head == null;
    }
};

const Mutex = struct {
    inner: c.pthread_mutex_t = .{},

    fn lock(mutex: *Mutex) void {
        const result = c.pthread_mutex_lock(&mutex.inner);
        std.debug.assert(result == .SUCCESS);
    }

    fn unlock(mutex: *Mutex) void {
        const result = c.pthread_mutex_unlock(&mutex.inner);
        std.debug.assert(result == .SUCCESS);
    }
};

/// Run queue of the CPU pool. Any thread may add to it; CPU threads sleep on
/// it while it is empty.
const CpuQueue = struct {
    mutex: Mutex = .{},
    not_empty: c.pthread_cond_t = .{},
    queue: Queue = .{},
    closed: bool = false,

    fn push(q: *CpuQueue, task: *Task) void {
        q.mutex.lock();
        q.queue.push(task);
        q.mutex.unlock();
        _ = c.pthread_cond_signal(&q.not_empty);
    }

    /// Waits for a task. Returns null once the queue has been closed.
    fn pop(q: *CpuQueue) ?*Task {
        q.mutex.lock();
        defer q.mutex.unlock();
        while (true) {
            if (q.queue.pop()) |task| return task;
            if (q.closed) return null;
            _ = c.pthread_cond_wait(&q.not_empty, &q.mutex.inner);
        }
    }

    /// For a thread whose task has used up its slice: puts `task` at the back
    /// and returns the task at the front. With nothing else queued that is
    /// `task` itself, which then keeps its thread without waking an idle one.
    fn rotate(q: *CpuQueue, task: *Task) *Task {
        q.mutex.lock();
        defer q.mutex.unlock();
        if (q.queue.isEmpty()) return task;
        q.queue.push(task);
        return q.queue.pop().?;
    }

    fn close(q: *CpuQueue) void {
        q.mutex.lock();
        q.closed = true;
        q.mutex.unlock();
        _ = c.pthread_cond_broadcast(&q.not_empty);
    }
};

/// Tasks that CPU threads hand back to the IO thread.
const Inbox = struct {
    mutex: Mutex = .{},
    queue: Queue = .{},

    /// Returns whether the inbox was empty before.
    fn push(inbox: *Inbox, task: *Task) bool {
        inbox.mutex.lock();
        const was_empty = inbox.queue.isEmpty();
        inbox.queue.push(task);
        inbox.mutex.unlock();
        return was_empty;
    }

    /// Empties the inbox, returning what was in it.
    fn take(inbox: *Inbox) Queue {
        inbox.mutex.lock();
        const arrivals = inbox.queue;
        inbox.queue = .{};
        inbox.mutex.unlock();
        return arrivals;
    }
};

// ---------------------------------------------------------------- memory ---

/// Bytes `init` needs. Layout:
///
///   [ Task × fibers | pthread_t × threads | pollfd × (fibers + 1) ]
///   [ guard page | fiber stack ] × fibers
///   [ guard page | thread stack ] × threads
///
/// Stacks grow down, so each guard page sits below its stack and an overflow
/// faults instead of scribbling over a neighbour.
pub fn memoryRequired(options: Options) usize {
    const page = std.heap.pageSize();
    return bookkeepingSize(options, page) +
        options.fiber_count * (page + std.mem.alignForward(usize, options.fiber_stack_size, page)) +
        options.cpu_threads * (page + std.mem.alignForward(usize, thread_stack_size, page));
}

fn bookkeepingSize(options: Options, page: usize) usize {
    return std.mem.alignForward(
        usize,
        options.fiber_count * @sizeOf(Task) +
            options.cpu_threads * @sizeOf(c.pthread_t) +
            (options.fiber_count + 1) * @sizeOf(posix.pollfd),
        page,
    );
}

const PageAligned = []align(std.heap.page_size_min) u8;

/// Takes a guard page and a stack of `len` bytes off the front of `rest`.
fn carveStack(rest: *PageAligned, len: usize, page: usize) error{GuardPageFailed}!PageAligned {
    const guard = rest.*[0..page];
    if (c.mprotect(guard.ptr, guard.len, .{}) != 0) return error.GuardPageFailed;

    const stack: PageAligned = @alignCast(rest.*[page..][0..len]);
    // Touch every page now, so the kernel commits the memory at startup
    // rather than at a page fault in the middle of some request.
    @memset(stack, 0);

    rest.* = @alignCast(rest.*[page + len ..]);
    return stack;
}

pub const InitError = error{ PipeFailed, GuardPageFailed, ThreadStartFailed };

/// Lays the scheduler out inside `memory` and starts the CPU threads.
/// `memory` must be zeroed, page-aligned and at least `memoryRequired` bytes
/// long, and stays in use until `deinit`. The scheduler must not be moved:
/// the tasks and the CPU threads point at it.
pub fn init(sched: *Scheduler, memory: PageAligned, options: Options) InitError!void {
    const page = std.heap.pageSize();
    std.debug.assert(options.cpu_threads >= 1);
    std.debug.assert(memory.len >= memoryRequired(options));

    // The three arrays, most strictly aligned first so they pack without padding.
    comptime std.debug.assert(@alignOf(Task) >= @alignOf(c.pthread_t));
    comptime std.debug.assert(@alignOf(c.pthread_t) >= @alignOf(posix.pollfd));
    const threads_offset = options.fiber_count * @sizeOf(Task);
    const pollfds_offset = threads_offset + options.cpu_threads * @sizeOf(c.pthread_t);
    const tasks: [*]Task = @ptrCast(memory.ptr);
    const threads: [*]c.pthread_t = @ptrCast(@alignCast(memory.ptr + threads_offset));
    const pollfds: [*]posix.pollfd = @ptrCast(@alignCast(memory.ptr + pollfds_offset));

    var wake_pipe: [2]posix.fd_t = undefined;
    if (c.pipe(&wake_pipe) != 0) return error.PipeFailed;

    sched.* = .{
        .tasks = tasks[0..options.fiber_count],
        .threads = threads[0..0], // grows as threads start
        .pollfds = pollfds[0 .. options.fiber_count + 1],
        .wake_read = wake_pipe[0],
        .wake_write = wake_pipe[1],
    };
    errdefer sched.deinit();

    var rest: PageAligned = @alignCast(memory[bookkeepingSize(options, page)..]);

    const fiber_stack_len = std.mem.alignForward(usize, options.fiber_stack_size, page);
    for (sched.tasks) |*task| {
        task.* = .{
            .sched = sched,
            .fiber = undefined,
            .stack = try carveStack(&rest, fiber_stack_len, page),
            .entry = undefined,
            .arg = 0,
            .next = null,
            .class = .io,
            .request = .reschedule,
            .slice_start_ns = 0,
            .state = .free,
            .fd = -1,
            .events = 0,
            .deadline_ns = no_deadline,
            .timed_out = false,
        };
        sched.free.push(task);
    }

    const thread_stack_len = std.mem.alignForward(usize, thread_stack_size, page);
    for (0..options.cpu_threads) |index| {
        const stack = try carveStack(&rest, thread_stack_len, page);
        try startThread(&threads[index], stack, options.cpu_thread_priority, sched);
        sched.threads = threads[0 .. index + 1];
    }
}

/// Starts a CPU thread on a stack we supply, so that the thread's stack is
/// part of the one block too.
fn startThread(
    thread: *c.pthread_t,
    stack: PageAligned,
    priority: Priority,
    sched: *Scheduler,
) error{ThreadStartFailed}!void {
    var attributes: c.pthread_attr_t = undefined;
    if (c.pthread_attr_init(&attributes) != .SUCCESS) return error.ThreadStartFailed;
    defer _ = c.pthread_attr_destroy(&attributes);
    if (c.pthread_attr_setstack(&attributes, stack.ptr, stack.len) != .SUCCESS) return error.ThreadStartFailed;
    if (priority == .low and comptime builtin.os.tag.isDarwin()) {
        // "Utility" is the quality-of-service class one step below the
        // default that the IO thread runs at.
        if (c.pthread_attr_set_qos_class_np(&attributes, .UTILITY, 0) != 0) return error.ThreadStartFailed;
    }
    if (c.pthread_create(thread, &attributes, cpuThreadMain, sched) != .SUCCESS) return error.ThreadStartFailed;
}

/// Stops and joins the CPU threads. Call only when no fiber is alive.
pub fn deinit(sched: *Scheduler) void {
    sched.cpu_queue.close();
    for (sched.threads) |thread| _ = c.pthread_join(thread, null);
    _ = c.close(sched.wake_read);
    _ = c.close(sched.wake_write);
}

// ------------------------------------------------------------ IO thread ---

/// Starts `entry(task, arg)` on a free fiber, as an IO-class fiber. Call from
/// the IO thread: before `run`, or from an IO-class fiber.
pub fn spawn(sched: *Scheduler, entry: Entry, arg: usize) error{NoFreeFiber}!void {
    const task = sched.free.pop() orelse return error.NoFreeFiber;
    task.entry = entry;
    task.arg = arg;
    task.class = .io;
    task.fiber.init(task.stack, taskMain);
    sched.live += 1;
    sched.makeReady(task);
}

fn taskMain(fiber: *Fiber) void {
    const task: *Task = @fieldParentPtr("fiber", fiber);
    task.entry(task, task.arg);
}

/// Runs fibers until every one of them has finished. The calling thread
/// becomes the IO thread.
pub fn run(sched: *Scheduler) void {
    while (sched.live > 0) {
        // Sleep in the kernel only when no IO-class fiber is ready to run.
        sched.pollIo(if (sched.io_ready.isEmpty()) .block else .dont_block);

        // Run the ready fibers, including any that become ready along the
        // way, so a connection accepted in this pass is also served in it.
        // The budget makes sure we get back to `poll` even if fibers keep
        // yielding to each other.
        var budget = sched.tasks.len;
        while (budget > 0) : (budget -= 1) {
            const task = sched.io_ready.pop() orelse break;
            task.state = .running;
            task.slice_start_ns = now();
            task.fiber.switchIn();
            sched.file(task);
        }
    }
}

/// Decides what happens to a fiber that has just stopped running, on this
/// thread or on a CPU thread, according to what it asked for.
fn file(sched: *Scheduler, task: *Task) void {
    if (task.fiber.finished) {
        task.state = .free;
        sched.free.push(task);
        sched.live -= 1;
        if (sched.fiber_waiter) |waiter| {
            sched.fiber_waiter = null;
            sched.makeReady(waiter);
        }
        return;
    }
    switch (task.request) {
        .reschedule => switch (task.class) {
            .io => sched.makeReady(task),
            .cpu => {
                task.state = .on_cpu_thread;
                sched.cpu_queue.push(task);
            },
        },
        .wait_io => {
            task.state = .waiting_io;
            sched.waiting_io += 1;
        },
        .wait_fiber => {
            std.debug.assert(sched.fiber_waiter == null);
            task.state = .waiting_fiber;
            sched.fiber_waiter = task;
        },
    }
}

fn makeReady(sched: *Scheduler, task: *Task) void {
    task.state = .ready;
    sched.io_ready.push(task);
}

/// Waits for file descriptors, deadlines and the inbox, and makes the fibers
/// concerned ready to run.
///
/// The poll set is rebuilt on each call. That is O(fibers) per pass, which is
/// the right trade for a small fixed pool; a large one would register with
/// kqueue/epoll instead.
fn pollIo(sched: *Scheduler, mode: enum { block, dont_block }) void {
    // Slot 0 is the wake pipe. The waiting fibers follow, in task order.
    sched.pollfds[0] = .{ .fd = sched.wake_read, .events = posix.POLL.IN, .revents = 0 };
    var count: usize = 1;
    var nearest_deadline_ns: u64 = no_deadline;
    if (sched.waiting_io > 0) {
        for (sched.tasks) |*task| {
            if (task.state != .waiting_io) continue;
            sched.pollfds[count] = .{ .fd = task.fd, .events = task.events, .revents = 0 };
            nearest_deadline_ns = @min(nearest_deadline_ns, task.deadline_ns);
            count += 1;
        }
    }

    const timeout_ms: i32 = switch (mode) {
        .dont_block => 0,
        .block => if (nearest_deadline_ns == no_deadline) -1 else timeout: {
            // Round up, or we would spin through the last partial millisecond.
            const remaining_ns = nearest_deadline_ns -| now();
            break :timeout std.math.lossyCast(i32, (remaining_ns + std.time.ns_per_ms - 1) / std.time.ns_per_ms);
        },
    };
    _ = posix.poll(sched.pollfds[0..count], timeout_ms) catch @panic("poll failed");

    if (count > 1) {
        // Same walk, same order: the nth waiting task owns slot n.
        const now_ns = now();
        var slot: usize = 1;
        for (sched.tasks) |*task| {
            if (task.state != .waiting_io) continue;
            const ready = sched.pollfds[slot].revents != 0;
            slot += 1;
            if (!ready and now_ns < task.deadline_ns) continue;

            task.timed_out = !ready;
            sched.waiting_io -= 1;
            sched.makeReady(task);
        }
    }

    // Last, because filing an arrival can add to the set of waiting tasks,
    // which the walk above must see exactly as it was when polled.
    if (sched.pollfds[0].revents != 0) sched.receiveFromCpuThreads();
}

fn receiveFromCpuThreads(sched: *Scheduler) void {
    // Empty the pipe before the inbox. A task that arrives after we take the
    // inbox then leaves a byte behind, and the next poll returns at once.
    // One read is enough: senders write a byte only when the inbox goes from
    // empty to occupied, so no more than two can be pending.
    var bytes: [8]u8 = undefined;
    _ = c.read(sched.wake_read, &bytes, bytes.len);

    var arrivals = sched.inbox.take();
    while (arrivals.pop()) |task| sched.file(task);
}

// ----------------------------------------------------------- CPU threads ---

fn cpuThreadMain(arg: ?*anyopaque) callconv(.c) ?*anyopaque {
    const sched: *Scheduler = @ptrCast(@alignCast(arg.?));
    var next = sched.cpu_queue.pop();
    while (next) |task| {
        task.slice_start_ns = now();
        task.fiber.switchIn();
        // The fiber is off its stack now, so another thread may have it.
        if (!task.fiber.finished and task.class == .cpu) {
            // Used up its slice with more to compute.
            next = sched.cpu_queue.rotate(task);
        } else {
            // Finished, or heading back to the IO thread.
            sched.sendToIoThread(task);
            next = sched.cpu_queue.pop();
        }
    }
    return null;
}

fn sendToIoThread(sched: *Scheduler, task: *Task) void {
    const was_empty = sched.inbox.push(task);
    // The IO thread may be asleep in `poll`. It only needs waking for the
    // first task: it empties the whole inbox when it looks.
    if (was_empty) _ = c.write(sched.wake_write, "!", 1);
}

/// Monotonic clock, in nanoseconds. Deadlines are measured against this.
pub fn now() u64 {
    var ts: c.timespec = undefined;
    _ = c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

// ----------------------------------------------------------------- tests ---

const testing = std.testing;

const TestScheduler = struct {
    memory: PageAligned,
    sched: Scheduler,

    fn init(t: *TestScheduler, fiber_count: usize, cpu_threads: usize) !void {
        return t.initOptions(.{
            .fiber_count = fiber_count,
            .fiber_stack_size = 64 * 1024,
            .cpu_threads = cpu_threads,
        });
    }

    fn initOptions(t: *TestScheduler, options: Options) !void {
        t.memory = try posix.mmap(
            null,
            memoryRequired(options),
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        );
        errdefer posix.munmap(t.memory);
        try t.sched.init(t.memory, options);
    }

    fn deinit(t: *TestScheduler) void {
        t.sched.deinit();
        posix.munmap(t.memory);
    }
};

/// Fibers append a letter here each time they get to run.
const Trace = struct {
    buffer: [64]u8 = undefined,
    len: usize = 0,

    fn add(trace: *Trace, letter: u8) void {
        trace.buffer[trace.len] = letter;
        trace.len += 1;
    }

    fn slice(trace: *const Trace) []const u8 {
        return trace.buffer[0..trace.len];
    }
};

fn makePipe() ![2]posix.fd_t {
    var pipe: [2]posix.fd_t = undefined;
    try testing.expectEqual(0, c.pipe(&pipe));
    return pipe;
}

fn closePipe(pipe: [2]posix.fd_t) void {
    for (pipe) |fd| _ = c.close(fd);
}

/// Long enough that only a scheduling bug makes a test wait this long.
const test_patience_ns = 2 * std.time.ns_per_s;

test "spawn fails once the pool is used up, and finished fibers are reused" {
    var t: TestScheduler = undefined;
    try t.init(2, 1);
    defer t.deinit();

    const Fns = struct {
        var runs: u32 = 0;
        fn work(_: *Task, _: usize) void {
            runs += 1;
        }
    };

    try t.sched.spawn(Fns.work, 0);
    try t.sched.spawn(Fns.work, 0);
    try testing.expectError(error.NoFreeFiber, t.sched.spawn(Fns.work, 0));
    t.sched.run();

    try t.sched.spawn(Fns.work, 0);
    t.sched.run();
    try testing.expectEqual(3, Fns.runs);
}

test "IO-class fibers that yield take turns" {
    var t: TestScheduler = undefined;
    try t.init(2, 1);
    defer t.deinit();

    const Fns = struct {
        var trace: Trace = .{};
        fn work(task: *Task, letter: usize) void {
            for (0..3) |_| {
                trace.add(@intCast(letter));
                task.yield();
            }
        }
    };

    try t.sched.spawn(Fns.work, 'a');
    try t.sched.spawn(Fns.work, 'b');
    t.sched.run();
    try testing.expectEqualStrings("ababab", Fns.trace.slice());
}

test "a fiber waiting on a file descriptor wakes when it becomes ready" {
    var t: TestScheduler = undefined;
    try t.init(2, 1);
    defer t.deinit();

    const Fns = struct {
        var trace: Trace = .{};
        var pipe: [2]posix.fd_t = undefined;

        fn reader(task: *Task, _: usize) void {
            trace.add('r');
            task.waitReadable(pipe[0], no_deadline) catch unreachable;
            trace.add('R');
        }

        fn writer(_: *Task, _: usize) void {
            trace.add('w');
            _ = c.write(pipe[1], "x", 1);
        }
    };

    Fns.pipe = try makePipe();
    defer closePipe(Fns.pipe);

    try t.sched.spawn(Fns.reader, 0);
    try t.sched.spawn(Fns.writer, 0);
    t.sched.run();
    try testing.expectEqualStrings("rwR", Fns.trace.slice());
}

test "an IO wait gives up at its deadline" {
    var t: TestScheduler = undefined;
    try t.init(1, 1);
    defer t.deinit();

    const Fns = struct {
        var pipe: [2]posix.fd_t = undefined;
        var result: ?error{Timeout}!void = null;

        fn reader(task: *Task, _: usize) void {
            result = task.waitReadable(pipe[0], now() + 20 * std.time.ns_per_ms);
        }
    };

    Fns.pipe = try makePipe();
    defer closePipe(Fns.pipe);

    const start = now();
    try t.sched.spawn(Fns.reader, 0);
    t.sched.run();
    try testing.expectError(error.Timeout, Fns.result.?);
    try testing.expect(now() - start >= 20 * std.time.ns_per_ms);
}

test "checkpoint yields only after a full time slice" {
    var t: TestScheduler = undefined;
    try t.init(2, 1);
    defer t.deinit();

    const Fns = struct {
        var trace: Trace = .{};

        fn spinner(task: *Task, _: usize) void {
            trace.add('1');
            task.checkpoint(); // slice barely started: keeps running
            trace.add('2');
            while (now() - task.slice_start_ns < time_slice_ns) {}
            task.checkpoint(); // slice spent: the other fiber gets a turn
            trace.add('3');
        }

        fn other(_: *Task, _: usize) void {
            trace.add('o');
        }
    };

    try t.sched.spawn(Fns.spinner, 0);
    try t.sched.spawn(Fns.other, 0);
    t.sched.run();
    try testing.expectEqualStrings("12o3", Fns.trace.slice());
}

test "moveTo carries a fiber to a CPU thread and back" {
    var t: TestScheduler = undefined;
    try t.init(1, 1);
    defer t.deinit();

    const Fns = struct {
        var before: c.pthread_t = undefined;
        var during: c.pthread_t = undefined;
        var after: c.pthread_t = undefined;
        var sum: u64 = 0;

        fn work(task: *Task, _: usize) void {
            // A local that has to survive both trips intact.
            var local: u64 = 40;
            before = c.pthread_self();
            task.moveTo(.cpu);
            during = c.pthread_self();
            local += 1;
            task.moveTo(.io);
            after = c.pthread_self();
            sum = local + 1;
        }
    };

    try t.sched.spawn(Fns.work, 0);
    t.sched.run();
    const io_thread = c.pthread_self();
    try testing.expectEqual(io_thread, Fns.before);
    try testing.expect(Fns.during != io_thread);
    try testing.expectEqual(io_thread, Fns.after);
    try testing.expectEqual(42, Fns.sum);
}

test "a fiber may finish on a CPU thread" {
    var t: TestScheduler = undefined;
    try t.init(1, 1);
    defer t.deinit();

    const Fns = struct {
        var runs = std.atomic.Value(u32).init(0);
        fn work(task: *Task, _: usize) void {
            task.moveTo(.cpu);
            _ = runs.fetchAdd(1, .monotonic);
        }
    };

    // The pool has one fiber, so the second spawn needs the first released.
    for (0..2) |_| {
        try t.sched.spawn(Fns.work, 0);
        t.sched.run();
    }
    try testing.expectEqual(2, Fns.runs.load(.monotonic));
}

test "waiting for IO on a CPU thread resumes the fiber on the IO thread" {
    var t: TestScheduler = undefined;
    try t.init(1, 1);
    defer t.deinit();

    const Fns = struct {
        var pipe: [2]posix.fd_t = undefined;
        var resumed_on: c.pthread_t = undefined;

        fn work(task: *Task, _: usize) void {
            task.moveTo(.cpu);
            task.waitReadable(pipe[0], no_deadline) catch unreachable;
            resumed_on = c.pthread_self();
        }
    };

    Fns.pipe = try makePipe();
    defer closePipe(Fns.pipe);
    _ = c.write(Fns.pipe[1], "x", 1);

    try t.sched.spawn(Fns.work, 0);
    t.sched.run();
    try testing.expectEqual(c.pthread_self(), Fns.resumed_on);
}

test "CPU-class fibers run in parallel on separate threads" {
    var t: TestScheduler = undefined;
    try t.init(2, 2);
    defer t.deinit();

    const Fns = struct {
        var computing = std.atomic.Value(u32).init(0);
        var overlapped = std.atomic.Value(u32).init(0);

        fn work(task: *Task, _: usize) void {
            task.moveTo(.cpu);
            _ = computing.fetchAdd(1, .monotonic);
            // Spin without yielding. The other fiber can only show up while
            // we are here if it has a thread of its own.
            const give_up_ns = now() + test_patience_ns;
            while (computing.load(.monotonic) < 2 and now() < give_up_ns) {}
            if (computing.load(.monotonic) == 2) _ = overlapped.fetchAdd(1, .monotonic);
        }
    };

    try t.sched.spawn(Fns.work, 0);
    try t.sched.spawn(Fns.work, 0);
    t.sched.run();
    try testing.expectEqual(2, Fns.overlapped.load(.monotonic));
}

test "checkpoint shares one CPU thread between CPU-class fibers" {
    var t: TestScheduler = undefined;
    try t.init(2, 1);
    defer t.deinit();

    const Fns = struct {
        var other_ran = std.atomic.Value(bool).init(false);
        var saw_other_run = false;

        fn hog(task: *Task, _: usize) void {
            task.moveTo(.cpu);
            // Holds the only CPU thread until the other fiber has run, which
            // can only happen if checkpoint lets it in.
            const give_up_ns = now() + test_patience_ns;
            while (!other_ran.load(.monotonic) and now() < give_up_ns) task.checkpoint();
            saw_other_run = other_ran.load(.monotonic);
        }

        fn other(task: *Task, _: usize) void {
            task.moveTo(.cpu);
            other_ran.store(true, .monotonic);
        }
    };

    try t.sched.spawn(Fns.hog, 0);
    try t.sched.spawn(Fns.other, 0);
    t.sched.run();
    try testing.expect(Fns.saw_other_run);
}

test "low priority puts the CPU threads below the IO thread" {
    if (comptime !builtin.os.tag.isDarwin()) return error.SkipZigTest;

    var t: TestScheduler = undefined;
    try t.initOptions(.{
        .fiber_count = 1,
        .fiber_stack_size = 64 * 1024,
        .cpu_threads = 1,
        .cpu_thread_priority = .low,
    });
    defer t.deinit();

    const Fns = struct {
        var cpu_thread_class: c.qos_class_t = .UNSPECIFIED;

        fn work(task: *Task, _: usize) void {
            task.moveTo(.cpu);
            var relative_priority: c_int = 0;
            _ = c.pthread_get_qos_class_np(c.pthread_self(), &cpu_thread_class, &relative_priority);
        }
    };

    try t.sched.spawn(Fns.work, 0);
    t.sched.run();
    try testing.expectEqual(c.qos_class_t.UTILITY, Fns.cpu_thread_class);
}

test "the IO thread keeps serving while a CPU thread is busy" {
    var t: TestScheduler = undefined;
    try t.init(2, 1);
    defer t.deinit();

    const Fns = struct {
        var pipe: [2]posix.fd_t = undefined;
        var reader_done = std.atomic.Value(bool).init(false);
        var reader_finished_first = false;

        fn reader(task: *Task, _: usize) void {
            task.waitReadable(pipe[0], no_deadline) catch unreachable;
            reader_done.store(true, .monotonic);
        }

        fn cruncher(task: *Task, _: usize) void {
            task.moveTo(.cpu);
            // Make the reader's fd ready, then compute without ever yielding
            // until the reader has been served.
            _ = c.write(pipe[1], "x", 1);
            const give_up_ns = now() + test_patience_ns;
            while (!reader_done.load(.monotonic) and now() < give_up_ns) {}
            reader_finished_first = reader_done.load(.monotonic);
        }
    };

    Fns.pipe = try makePipe();
    defer closePipe(Fns.pipe);

    try t.sched.spawn(Fns.reader, 0);
    try t.sched.spawn(Fns.cruncher, 0);
    t.sched.run();
    try testing.expect(Fns.reader_finished_first);
}

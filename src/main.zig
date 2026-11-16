//! pingpong: a small HTTP server built on fibers.
//!
//!   GET /ping      "pong"                 IO-bound: nothing but socket IO
//!   GET /cpu?n=N   count of primes < N    CPU-bound: computes between the IO
//!
//! Every request runs on its own fiber. One IO thread accepts connections and
//! does all the socket IO. A /cpu request hops to a pool of CPU threads for
//! its computation and back again to send the answer, so computations run in
//! parallel and never hold up a /ping.
//!
//! Memory: a single mmap at startup holds every fiber stack, every CPU
//! thread's stack and all scheduler state; request buffers live on the fiber
//! stacks. Nothing is allocated after that. The price is a hard cap of
//! `config.max_requests` requests in flight; connections beyond it wait in
//! the kernel's accept queue.

const std = @import("std");
const Scheduler = @import("fiber").Scheduler;
const Task = Scheduler.Task;
const net = @import("net.zig");

const config = struct {
    const default_port = 8080;

    /// Requests in flight at once. Each one occupies a fiber.
    const max_requests = 64;
    const fiber_stack_size = 256 * 1024;
    /// Threads for CPU-bound work. Null means one per core, less one core
    /// left for the IO thread.
    const cpu_threads: ?usize = null;
    /// Low keeps pings quick even with every core computing, at the cost of
    /// some throughput for the computations. `.normal` makes the opposite
    /// trade. (Has an effect on macOS only.)
    const cpu_thread_priority: Scheduler.Priority = .low;

    /// Holds the request line and headers; lives on the fiber's stack.
    const request_buffer_size = 4096;
    /// Connections the kernel queues for us while every fiber is busy.
    const listen_backlog = 128;

    /// How long a client gets to send its request, and again to accept the
    /// response. Without it one silent client would hold a fiber forever.
    const io_timeout_ns = 5 * std.time.ns_per_s;

    /// About a second of trial division in a release build.
    const cpu_default_n = 10_000_000;
    const cpu_max_n = 50_000_000;
};

pub fn main(init: std.process.Init.Minimal) void {
    const port = portFromArgs(init.args);

    // A write to a socket whose client has gone should fail, not kill us.
    std.posix.sigaction(.PIPE, &.{
        .handler = .{ .handler = std.posix.SIG.IGN },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    }, null);

    const options: Scheduler.Options = .{
        .fiber_count = config.max_requests + 1, // one more for the accept loop
        .fiber_stack_size = config.fiber_stack_size,
        .cpu_threads = config.cpu_threads orelse @max(1, (std.Thread.getCpuCount() catch 2) - 1),
        .cpu_thread_priority = config.cpu_thread_priority,
    };

    // The one allocation. Everything the server will ever use is in here.
    const memory_size = Scheduler.memoryRequired(options);
    const memory = std.posix.mmap(
        null,
        memory_size,
        .{ .READ = true, .WRITE = true },
        .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
        -1,
        0,
    ) catch |err| std.process.fatal("cannot allocate {d} bytes: {t}", .{ memory_size, err });

    var sched: Scheduler = undefined;
    sched.init(memory, options) catch |err|
        std.process.fatal("cannot start the scheduler: {t}", .{err});

    const listener = net.listen(port, config.listen_backlog) catch |err|
        std.process.fatal("cannot listen on 127.0.0.1:{d}: {t}", .{ port, err });
    sched.spawn(acceptLoop, @intCast(listener)) catch unreachable;

    std.debug.print(
        "pingpong: http://127.0.0.1:{d}  ({d} request fibers, 1 IO thread, {d} CPU threads, {d} KiB allocated once)\n",
        .{ port, config.max_requests, options.cpu_threads, memory_size / 1024 },
    );
    sched.run();
}

fn portFromArgs(args: std.process.Args) u16 {
    var it = args.iterate();
    _ = it.skip(); // program name
    const text = it.next() orelse return config.default_port;
    return std.fmt.parseInt(u16, text, 10) catch std.process.fatal("invalid port: {s}", .{text});
}

fn acceptLoop(task: *Task, arg: usize) void {
    const listener: net.Socket = @intCast(arg);
    while (true) {
        // Only take a connection once there is a fiber to serve it. Until
        // then clients queue in the kernel: that is the backpressure.
        task.waitForFreeFiber();
        const client = net.accept(task, listener) catch |err|
            std.process.fatal("accept failed: {t}", .{err});
        task.sched.spawn(handleConnection, @intCast(client)) catch unreachable;
    }
}

fn handleConnection(task: *Task, arg: usize) void {
    const client: net.Socket = @intCast(arg);
    defer net.close(client);
    // A client that stalls, disconnects or sends garbage only loses its own
    // connection; there is nobody to report the error to.
    serve(task, client) catch {};
}

/// Reads one request, answers it, and returns. One request per connection.
fn serve(task: *Task, client: net.Socket) !void {
    const deadline_ns = Scheduler.now() + config.io_timeout_ns;
    var buffer: [config.request_buffer_size]u8 = undefined;
    var len: usize = 0;
    const head = while (true) {
        if (std.mem.find(u8, buffer[0..len], "\r\n\r\n")) |end| break buffer[0..end];
        if (len == buffer.len) {
            return respond(task, client, "431 Request Header Fields Too Large", "request too large\n");
        }
        const n = try net.read(task, client, buffer[len..], deadline_ns);
        if (n == 0) return; // the client hung up before finishing its request
        len += n;
    };

    // Request line: METHOD SP target SP version
    const request_line = head[0 .. std.mem.find(u8, head, "\r\n") orelse head.len];
    var words = std.mem.splitScalar(u8, request_line, ' ');
    const method = words.next() orelse "";
    const target = words.next() orelse "";
    const version = words.next() orelse "";
    if (target.len == 0 or !std.mem.startsWith(u8, version, "HTTP/1.")) {
        return respond(task, client, "400 Bad Request", "bad request\n");
    }
    if (!std.mem.eql(u8, method, "GET")) {
        return respond(task, client, "405 Method Not Allowed", "only GET is supported\n");
    }

    const path, const query = std.mem.cutScalar(u8, target, '?') orelse .{ target, "" };
    if (std.mem.eql(u8, path, "/ping")) {
        return respond(task, client, "200 OK", "pong\n");
    }
    if (std.mem.eql(u8, path, "/cpu")) {
        const n = parseCpuQuery(query) orelse {
            return respond(task, client, "400 Bad Request", std.fmt.comptimePrint(
                "usage: /cpu?n=N with N at most {d}\n",
                .{config.cpu_max_n},
            ));
        };

        // Compute on a CPU thread, leaving the IO thread free for other
        // requests, then come back: sockets belong to the IO thread.
        task.moveTo(.cpu);
        const count = countPrimes(task, n);
        task.moveTo(.io);

        var body: [64]u8 = undefined;
        return respond(task, client, "200 OK", try std.fmt.bufPrint(
            &body,
            "{d} primes below {d}\n",
            .{ count, n },
        ));
    }
    return respond(task, client, "404 Not Found", "try /ping or /cpu\n");
}

fn respond(task: *Task, client: net.Socket, status: []const u8, body: []const u8) !void {
    var buffer: [512]u8 = undefined;
    const response = try std.fmt.bufPrint(
        &buffer,
        "HTTP/1.1 {s}\r\n" ++
            "Content-Type: text/plain\r\n" ++
            "Content-Length: {d}\r\n" ++
            "Connection: close\r\n" ++
            "\r\n" ++
            "{s}",
        .{ status, body.len, body },
    );
    try net.writeAll(task, client, response, Scheduler.now() + config.io_timeout_ns);
}

/// Parses the query string of /cpu: empty, or "n=<number>".
fn parseCpuQuery(query: []const u8) ?u32 {
    if (query.len == 0) return config.cpu_default_n;
    const digits = std.mem.cutPrefix(u8, query, "n=") orelse return null;
    const n = std.fmt.parseInt(u32, digits, 10) catch return null;
    return if (n <= config.cpu_max_n) n else null;
}

/// Counts the primes below `limit` by trial division. Deliberately slow: it
/// stands in for any request that needs real CPU time.
fn countPrimes(task: *Task, limit: u32) u32 {
    var count: u32 = 0;
    var n: u32 = 2;
    while (n < limit) : (n += 1) {
        if (isPrime(n)) count += 1;
        // With more computations than CPU threads, take turns. This only
        // switches fibers once our time slice is used up; otherwise it is a
        // clock read.
        if (n % 256 == 0) task.checkpoint();
    }
    return count;
}

fn isPrime(n: u32) bool {
    if (n < 2) return false;
    if (n % 2 == 0) return n == 2;
    var divisor: u64 = 3;
    while (divisor * divisor <= n) : (divisor += 2) {
        if (n % divisor == 0) return false;
    }
    return true;
}

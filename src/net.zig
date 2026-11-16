//! Socket calls for fibers.
//!
//! Each call reads like ordinary blocking IO to the fiber making it.
//! Underneath, every socket is non-blocking, and a call that would block
//! parks the fiber in the scheduler until the socket is ready.
//!
//! Sockets belong to the IO thread. A fiber that has moved to a CPU thread
//! should move back before using one.

const std = @import("std");
const c = std.c;
const Scheduler = @import("fiber").Scheduler;
const Task = Scheduler.Task;

pub const Socket = std.posix.fd_t;

/// Opens a listening socket on 127.0.0.1:`port`.
pub fn listen(port: u16, backlog: u31) !Socket {
    const socket = c.socket(c.AF.INET, c.SOCK.STREAM, 0);
    if (socket < 0) return error.SocketFailed;
    errdefer close(socket);

    // Lets the server restart without waiting out old connections' TIME_WAIT.
    const on: c_int = 1;
    _ = c.setsockopt(socket, c.SOL.SOCKET, c.SO.REUSEADDR, &on, @sizeOf(c_int));

    const address: c.sockaddr.in = .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = std.mem.nativeToBig(u32, 0x7f00_0001),
    };
    if (c.bind(socket, @ptrCast(&address), @sizeOf(c.sockaddr.in)) != 0) {
        return if (c.errno(-1) == .ADDRINUSE) error.AddressInUse else error.BindFailed;
    }
    if (c.listen(socket, backlog) != 0) return error.ListenFailed;
    try setNonBlocking(socket);
    return socket;
}

/// Waits for the next incoming connection.
pub fn accept(task: *Task, listener: Socket) !Socket {
    while (true) {
        const socket = c.accept(listener, null, null);
        if (socket >= 0) {
            errdefer close(socket);
            try setNonBlocking(socket);
            return socket;
        }
        switch (c.errno(socket)) {
            .AGAIN => task.waitReadable(listener, Scheduler.no_deadline) catch unreachable,
            .INTR, .CONNABORTED => {},
            else => return error.AcceptFailed,
        }
    }
}

/// Reads at least one byte, unless the peer has closed the connection, in
/// which case it returns 0.
pub fn read(task: *Task, socket: Socket, buffer: []u8, deadline_ns: u64) !usize {
    while (true) {
        const n = c.read(socket, buffer.ptr, buffer.len);
        if (n >= 0) return @intCast(n);
        switch (c.errno(n)) {
            .AGAIN => try task.waitReadable(socket, deadline_ns),
            .INTR => {},
            else => return error.ReadFailed,
        }
    }
}

pub fn writeAll(task: *Task, socket: Socket, bytes: []const u8, deadline_ns: u64) !void {
    var rest = bytes;
    while (rest.len > 0) {
        const n = c.write(socket, rest.ptr, rest.len);
        if (n >= 0) {
            rest = rest[@intCast(n)..];
            continue;
        }
        switch (c.errno(n)) {
            .AGAIN => try task.waitWritable(socket, deadline_ns),
            .INTR => {},
            else => return error.WriteFailed,
        }
    }
}

pub fn close(socket: Socket) void {
    _ = c.close(socket);
}

fn setNonBlocking(socket: Socket) !void {
    const nonblock: c_int = @bitCast(c.O{ .NONBLOCK = true });
    const flags = c.fcntl(socket, c.F.GETFL);
    if (flags < 0 or c.fcntl(socket, c.F.SETFL, flags | nonblock) < 0) return error.FcntlFailed;
}

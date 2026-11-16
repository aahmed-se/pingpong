//! A small fiber library.
//!
//! `Fiber` is the mechanism: a separate call stack and a context switch.
//! `Scheduler` is the policy: a fixed pool of fibers, split into IO-bound and
//! CPU-bound run queues.
//!
//! Neither allocates. The scheduler is handed one block of memory at startup
//! and carves every fiber stack out of it.

pub const Fiber = @import("Fiber.zig");
pub const Scheduler = @import("Scheduler.zig");

test {
    _ = Fiber;
    _ = Scheduler;
}

//! A stackful fiber: its own call stack plus a hand-written context switch.
//!
//! Switching is symmetric and tiny. `swap` pushes the callee-saved registers
//! onto the current stack, stores the stack pointer, loads the other side's
//! stack pointer and pops that side's registers. The compiler already treats
//! every other register as clobbered by a call, so nothing else needs saving.
//!
//! The caller supplies the stack memory; nothing here allocates.

const std = @import("std");
const builtin = @import("builtin");

const Fiber = @This();

/// This fiber's stack pointer while it is switched out.
sp: usize,
/// Stack pointer of the code that switched this fiber in, while it runs.
caller_sp: usize,
entry: Entry,
/// Set once `entry` has returned. A finished fiber must be `init`ed again
/// before it can be switched in.
finished: bool,

pub const Entry = *const fn (fiber: *Fiber) void;

/// Prepares `fiber` to run `entry` on `stack` the first time it is switched in.
/// The fiber must not move afterwards: its address is baked into the stack.
pub fn init(fiber: *Fiber, stack: []u8, entry: Entry) void {
    // Build the frame `swap` expects to pop, so that the first switch "returns"
    // into the trampoline with the fiber pointer in a callee-saved register.
    const top = std.mem.alignBackward(usize, @intFromPtr(stack.ptr) + stack.len, 16);
    const frame: [*]usize = @ptrFromInt(top - arch.frame_words * @sizeOf(usize));
    @memset(frame[0..arch.frame_words], 0);
    frame[arch.fiber_slot] = @intFromPtr(fiber);
    frame[arch.return_slot] = @intFromPtr(&arch.trampoline);

    fiber.* = .{
        .sp = @intFromPtr(frame),
        .caller_sp = 0,
        .entry = entry,
        .finished = false,
    };
}

/// Runs the fiber until it calls `switchOut` or its entry function returns.
pub fn switchIn(fiber: *Fiber) void {
    std.debug.assert(!fiber.finished);
    swap(&fiber.caller_sp, &fiber.sp);
}

/// Called from inside the fiber: suspends it and returns to whoever called
/// `switchIn`. Execution continues here on the next `switchIn`.
pub fn switchOut(fiber: *Fiber) void {
    swap(&fiber.sp, &fiber.caller_sp);
}

/// First function on every fiber's stack.
fn main(fiber: *Fiber) callconv(.c) noreturn {
    fiber.entry(fiber);
    fiber.finished = true;
    fiber.switchOut();
    unreachable; // a finished fiber is never switched in again
}

/// Saves the current context, storing its stack pointer in `save`, then
/// resumes the context whose stack pointer is in `load`.
const swap: *const fn (save: *usize, load: *const usize) callconv(.c) void = @ptrCast(&arch.swap);

const arch = switch (builtin.cpu.arch) {
    .aarch64 => struct {
        // Saved frame, low to high: x19..x28, x29 (fp), x30 (lr), d8..d15.
        const frame_words = 20;
        const fiber_slot = 0; // x19
        const return_slot = 11; // x30

        fn swap() callconv(.naked) void {
            asm volatile (
                \\ sub sp, sp, #160
                \\ stp x19, x20, [sp, #0]
                \\ stp x21, x22, [sp, #16]
                \\ stp x23, x24, [sp, #32]
                \\ stp x25, x26, [sp, #48]
                \\ stp x27, x28, [sp, #64]
                \\ stp x29, x30, [sp, #80]
                \\ stp d8, d9, [sp, #96]
                \\ stp d10, d11, [sp, #112]
                \\ stp d12, d13, [sp, #128]
                \\ stp d14, d15, [sp, #144]
                \\ mov x9, sp
                \\ str x9, [x0]
                \\ ldr x9, [x1]
                \\ mov sp, x9
                \\ ldp x19, x20, [sp, #0]
                \\ ldp x21, x22, [sp, #16]
                \\ ldp x23, x24, [sp, #32]
                \\ ldp x25, x26, [sp, #48]
                \\ ldp x27, x28, [sp, #64]
                \\ ldp x29, x30, [sp, #80]
                \\ ldp d8, d9, [sp, #96]
                \\ ldp d10, d11, [sp, #112]
                \\ ldp d12, d13, [sp, #128]
                \\ ldp d14, d15, [sp, #144]
                \\ add sp, sp, #160
                \\ ret
            );
        }

        fn trampoline() callconv(.naked) void {
            asm volatile (
                \\ mov x0, x19
                \\ b %[main]
                :
                : [main] "X" (&main),
            );
        }
    },
    .x86_64 => struct {
        // Saved frame, low to high: r15, r14, r13, r12, rbx, rbp, return
        // address, and one padding word so the entry function sees the stack
        // alignment a `call` would have left.
        const frame_words = 8;
        const fiber_slot = 3; // r12
        const return_slot = 6;

        fn swap() callconv(.naked) void {
            asm volatile (
                \\ pushq %%rbp
                \\ pushq %%rbx
                \\ pushq %%r12
                \\ pushq %%r13
                \\ pushq %%r14
                \\ pushq %%r15
                \\ movq %%rsp, (%%rdi)
                \\ movq (%%rsi), %%rsp
                \\ popq %%r15
                \\ popq %%r14
                \\ popq %%r13
                \\ popq %%r12
                \\ popq %%rbx
                \\ popq %%rbp
                \\ retq
            );
        }

        fn trampoline() callconv(.naked) void {
            asm volatile (
                \\ movq %%r12, %%rdi
                \\ jmp %[main:P]
                :
                : [main] "X" (&main),
            );
        }
    },
    else => |a| @compileError("fiber context switch not implemented for " ++ @tagName(a)),
};

test "control ping-pongs between a fiber and its caller" {
    const Context = struct {
        var hits: u32 = 0;
        // Floating-point state must survive a switch too (d8..d15 on aarch64).
        var sum: f64 = 0;

        fn entry(fiber: *Fiber) void {
            var local: f64 = 0.5;
            for (0..3) |_| {
                hits += 1;
                sum += local;
                local *= 2;
                fiber.switchOut();
            }
        }
    };

    var stack: [64 * 1024]u8 align(16) = undefined;
    var fiber: Fiber = undefined;
    fiber.init(&stack, Context.entry);

    for (1..4) |expected| {
        fiber.switchIn();
        try std.testing.expectEqual(expected, Context.hits);
        try std.testing.expect(!fiber.finished);
    }
    try std.testing.expectEqual(0.5 + 1.0 + 2.0, Context.sum);

    fiber.switchIn(); // the loop ends and the entry function returns
    try std.testing.expect(fiber.finished);
}

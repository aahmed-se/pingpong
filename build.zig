const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The fiber library: context switch + scheduler. No dependency on the server.
    const fiber = b.addModule("fiber", .{
        .root_source_file = b.path("src/fiber/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    const exe = b.addExecutable(.{
        .name = "pingpong",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{.{ .name = "fiber", .module = fiber }},
        }),
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    run_cmd.addPassthruArgs();
    b.step("run", "Run the server (optional arg: port)").dependOn(&run_cmd.step);

    const fiber_tests = b.addRunArtifact(b.addTest(.{ .root_module = fiber }));
    b.step("test", "Run the fiber library tests").dependOn(&fiber_tests.step);
}

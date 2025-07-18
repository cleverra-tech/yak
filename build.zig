const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Add Wombat dependency using modern package manager
    const wombat_dep = b.dependency("wombat", .{
        .target = target,
        .optimize = optimize,
    });
    const wombat_module = wombat_dep.module("wombat");

    // Build Yak message broker
    const yak_exe = b.addExecutable(.{
        .name = "yak",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Build Yak CLI client
    const yak_cli = b.addExecutable(.{
        .name = "yak-cli",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cli/client.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Link with Wombat
    yak_exe.root_module.addImport("wombat", wombat_module);
    yak_cli.root_module.addImport("wombat", wombat_module);

    // Performance optimizations for release builds
    if (optimize == .ReleaseFast or optimize == .ReleaseSmall) {
        yak_exe.want_lto = true;
        yak_cli.want_lto = true;
    }

    b.installArtifact(yak_exe);
    b.installArtifact(yak_cli);

    // Create run steps
    const run_yak = b.addRunArtifact(yak_exe);
    run_yak.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_yak.addArgs(args);
    }

    const run_step = b.step("run", "Run the Yak message broker");
    run_step.dependOn(&run_yak.step);

    // Test suite
    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    unit_tests.root_module.addImport("wombat", wombat_module);

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    // Benchmark suite
    const benchmark_exe = b.addExecutable(.{
        .name = "yak-benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Add src as a module for benchmarks to import
    const yak_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    yak_module.addImport("wombat", wombat_module);
    benchmark_exe.root_module.addImport("yak", yak_module);
    benchmark_exe.root_module.addImport("wombat", wombat_module);
    b.installArtifact(benchmark_exe);

    const run_benchmark = b.addRunArtifact(benchmark_exe);
    const benchmark_step = b.step("benchmark", "Run performance benchmarks");
    benchmark_step.dependOn(&run_benchmark.step);

    // Integration tests
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    integration_tests.root_module.addImport("wombat", wombat_module);

    const run_integration_tests = b.addRunArtifact(integration_tests);
    const integration_step = b.step("integration", "Run integration tests");
    integration_step.dependOn(&run_integration_tests.step);

    // All tests
    const all_tests_step = b.step("test-all", "Run all tests");
    all_tests_step.dependOn(&run_unit_tests.step);
    all_tests_step.dependOn(&run_integration_tests.step);

    // Clean step
    const clean_step = b.step("clean", "Clean build artifacts");
    clean_step.dependOn(&b.addRemoveDirTree(b.path("zig-out")).step);
    clean_step.dependOn(&b.addRemoveDirTree(b.path(".zig-cache")).step);
}

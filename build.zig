const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const grpc_dep = b.dependency("grpc", .{ .target = target, .optimize = optimize });
    const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });
    const stdx_dep = b.dependency("stdx", .{ .target = target, .optimize = optimize });

    const mod = b.addModule("etcd", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "grpc", .module = grpc_dep.module("grpc") },
            .{ .name = "protobuf", .module = protobuf_dep.module("protobuf") },
            .{ .name = "stdx", .module = stdx_dep.module("stdx") },
        },
    });

    // A consumer configures this module and stops here. The demo and the
    // test runner stay on the development side of the line.
    if (b.pkg_hash.len != 0) return;

    const exe = b.addExecutable(.{
        .name = "etcd",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "etcd", .module = mod },
            },
        }),
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    run_cmd.addPassthruArgs();
    b.step("run", "Run the etcd demo").dependOn(&run_cmd.step);

    // The benchmark harness runs against a live etcd. It is dev-only, like
    // the demo and the tests.
    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/bench.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "etcd", .module = mod },
            },
        }),
    });
    b.installArtifact(bench);
    const run_bench = b.addRunArtifact(bench);
    run_bench.addPassthruArgs();
    b.step("bench", "Run the benchmark against a live etcd").dependOn(&run_bench.step);

    // The test runner and the integration harness are dev-only. Both are
    // lazy dependencies so a consumer of the package never fetches them.
    const testingx_dep = b.lazyDependency("testingx", .{ .target = target }) orelse
        return;
    const test_runner: std.Build.Step.Compile.TestRunner = .{
        .path = testingx_dep.path("src/runner.zig"),
        .mode = .simple,
    };

    const tests = b.addTest(.{
        .root_module = mod,
        .test_runner = test_runner,
    });
    const run_tests = b.addRunArtifact(tests);
    run_tests.addPassthruArgs();
    b.step("test", "Run the unit tests").dependOn(&run_tests.step);

    // `zig build test-integration -Detcd-endpoint=127.0.0.1:2379`.
    // The justfile starts a throwaway etcd and passes the endpoint.
    const endpoint = b.option([]const u8, "etcd-endpoint", "Endpoint for the integration tests");
    if (endpoint) |value| {
        const config = b.addOptions();
        config.addOption([]const u8, "endpoint", value);
        config.addOption(
            []const u8,
            "auth_endpoint",
            b.option([]const u8, "etcd-auth-endpoint", "Auth-enabled endpoint") orelse "",
        );
        config.addOption(
            []const u8,
            "auth_user",
            b.option([]const u8, "etcd-auth-user", "Auth-enabled user") orelse "",
        );
        config.addOption(
            []const u8,
            "auth_password",
            b.option([]const u8, "etcd-auth-password", "Auth-enabled password") orelse "",
        );

        const integration_mod = b.createModule(.{
            .root_source_file = b.path("src/integration.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "etcd", .module = mod },
                .{ .name = "config", .module = config.createModule() },
            },
        });
        const integration_tests = b.addTest(.{
            .root_module = integration_mod,
            .test_runner = test_runner,
        });
        const run_integration = b.addRunArtifact(integration_tests);
        run_integration.addPassthruArgs();
        b.step("test-integration", "Run the tests against a live etcd")
            .dependOn(&run_integration.step);
    }
}

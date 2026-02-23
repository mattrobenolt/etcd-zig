const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const nghttp2_dep = b.dependency("nghttp2", .{});

    // Build nghttp2 C sources as a static library.
    const nghttp2 = b.addLibrary(.{
        .name = "nghttp2",
        .linkage = .static,
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    nghttp2.addIncludePath(nghttp2_dep.path("lib/includes"));
    nghttp2.addIncludePath(nghttp2_dep.path("lib"));

    const nghttp2_sources: []const []const u8 = &.{
        "nghttp2_alpn.c",
        "nghttp2_buf.c",
        "nghttp2_callbacks.c",
        "nghttp2_debug.c",
        "nghttp2_extpri.c",
        "nghttp2_frame.c",
        "nghttp2_hd.c",
        "nghttp2_hd_huffman.c",
        "nghttp2_hd_huffman_data.c",
        "nghttp2_helper.c",
        "nghttp2_http.c",
        "nghttp2_map.c",
        "nghttp2_mem.c",
        "nghttp2_option.c",
        "nghttp2_outbound_item.c",
        "nghttp2_pq.c",
        "nghttp2_priority_spec.c",
        "nghttp2_queue.c",
        "nghttp2_ratelim.c",
        "nghttp2_rcbuf.c",
        "nghttp2_session.c",
        "nghttp2_stream.c",
        "nghttp2_submit.c",
        "nghttp2_time.c",
        "nghttp2_version.c",
        "sfparse.c",
    };

    const non_windows_flags: []const []const u8 = &.{
        "-DHAVE_ARPA_INET_H",
        "-DHAVE_NETINET_IN_H",
        "-DHAVE_CLOCK_GETTIME",
        "-DHAVE_DECL_CLOCK_MONOTONIC=1",
    };
    const no_flags: []const []const u8 = &.{};

    nghttp2.addCSourceFiles(.{
        .root = nghttp2_dep.path("lib"),
        .files = nghttp2_sources,
        .flags = if (target.result.os.tag != .windows) non_windows_flags else no_flags,
    });

    // Protobuf runtime module.
    const protobuf_dep = b.dependency("protobuf", .{});
    const protobuf_mod = protobuf_dep.module("protobuf");

    // Library module — the public API of this package.
    const mod = b.addModule("etcd", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "protobuf", .module = protobuf_mod },
        },
    });
    mod.addIncludePath(nghttp2_dep.path("lib/includes"));
    mod.linkLibrary(nghttp2);

    // Executable.
    const exe = b.addExecutable(.{
        .name = "etcd",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "etcd", .module = mod },
                .{ .name = "protobuf", .module = protobuf_mod },
            },
        }),
    });
    exe.root_module.addIncludePath(nghttp2_dep.path("lib/includes"));
    exe.root_module.linkLibrary(nghttp2);

    // Build the protoc-gen-zig plugin for codegen.
    const protoc_gen_zig = b.addExecutable(.{
        .name = "protoc-gen-zig",
        .root_module = b.createModule(.{
            .root_source_file = protobuf_dep.path("bootstrapped-generator/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "protobuf", .module = protobuf_mod },
            },
        }),
    });
    b.installArtifact(protoc_gen_zig);

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // Tests.
    const mod_tests = b.addTest(.{ .root_module = mod });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{ .root_module = exe.root_module });
    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);
}

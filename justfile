# etcd: an etcd client on baskingspot/grpc. `just --list` shows the recipes.

default:
    @just --list

# The unit tests, through the testingx runner. The timeout turns a hung
# socket test into a failure instead of a wait.
test:
    zig build test --summary all -- --timeout=2m

# The integration tests against a throwaway etcd on 127.0.0.1:22379.
test-integration:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v etcd >/dev/null || { echo "etcd is not on PATH; run inside nix develop" >&2; exit 1; }
    port=22379
    peer=22380
    auth_port=22479
    auth_peer=22480
    data=$(mktemp -d)
    auth_data=$(mktemp -d)
    etcd --data-dir "$data" \
        --listen-client-urls "http://127.0.0.1:${port}" \
        --advertise-client-urls "http://127.0.0.1:${port}" \
        --listen-peer-urls "http://127.0.0.1:${peer}" \
        --initial-advertise-peer-urls "http://127.0.0.1:${peer}" \
        --initial-cluster "default=http://127.0.0.1:${peer}" \
        >"$data/etcd.log" 2>&1 &
    pid=$!
    etcd --data-dir "$auth_data" \
        --listen-client-urls "http://127.0.0.1:${auth_port}" \
        --advertise-client-urls "http://127.0.0.1:${auth_port}" \
        --listen-peer-urls "http://127.0.0.1:${auth_peer}" \
        --initial-advertise-peer-urls "http://127.0.0.1:${auth_peer}" \
        --initial-cluster "default=http://127.0.0.1:${auth_peer}" \
        >"$auth_data/etcd.log" 2>&1 &
    auth_pid=$!
    trap 'kill "$pid" "$auth_pid" 2>/dev/null || true; wait "$pid" "$auth_pid" 2>/dev/null || true; rm -rf "$data" "$auth_data"' EXIT

    for p in "$port" "$auth_port"; do
        for _ in $(seq 1 100); do
            if (exec 3<>"/dev/tcp/127.0.0.1/${p}") 2>/dev/null; then
                exec 3>&-
                break
            fi
            sleep 0.1
        done
    done

    etcdctl --endpoints "http://127.0.0.1:${auth_port}" user add root:rootpw
    etcdctl --endpoints "http://127.0.0.1:${auth_port}" user grant-role root root
    etcdctl --endpoints "http://127.0.0.1:${auth_port}" auth enable

    zig build test-integration -Detcd-endpoint="127.0.0.1:${port}" \
        -Detcd-auth-endpoint="127.0.0.1:${auth_port}" \
        -Detcd-auth-user=root -Detcd-auth-password=rootpw \
        --summary all -- --timeout=2m --filter=integration

# Run the demo against an endpoint.
demo endpoint="127.0.0.1:2379":
    zig build run -- {{ endpoint }}

# Sequential Put, Range, and watch-event latency against a throwaway etcd.
# The disk stays out of the Put path, so the numbers describe the client.
bench:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v etcd >/dev/null || { echo "etcd is not on PATH; run inside nix develop" >&2; exit 1; }
    port=22389
    peer=22390
    data=$(mktemp -d)
    etcd --data-dir "$data" \
        --listen-client-urls "http://127.0.0.1:${port}" \
        --advertise-client-urls "http://127.0.0.1:${port}" \
        --listen-peer-urls "http://127.0.0.1:${peer}" \
        --initial-advertise-peer-urls "http://127.0.0.1:${peer}" \
        --initial-cluster "default=http://127.0.0.1:${peer}" \
        --unsafe-no-fsync \
        --log-level error \
        >"$data/etcd.log" 2>&1 &
    pid=$!
    trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; rm -rf "$data"' EXIT

    for _ in $(seq 1 100); do
        if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
            exec 3>&-
            break
        fi
        sleep 0.1
    done

    zig build bench -Doptimize=ReleaseFast -- "127.0.0.1:${port}" all 5000

fmt:
    zig fmt build.zig src bench

fmt-check:
    zig fmt --check build.zig src bench

lint:
    ziglint

ci: fmt-check lint test

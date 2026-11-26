#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
root="$PWD"
mkdir -p "$root/results"
exec > >(tee "$root/results/build.log") 2>&1
base=fd00bc3a2a02a614fca8241d16570801a3a08314
patch_hash=c3bc651a21578977a23acd054c38289aacd82e35b9ff5525f380bed48fec9658
printf '%s  candidate.patch\n' "$patch_hash" | shasum -a 256 -c
{
  sw_vers
  uname -a
  java -version
  printf 'baseline=%s candidate_sha256=%s\n' "$base" "$patch_hash"
  printf '4 server loops, 2us/hop, 4 and 20 connections, 5 AB/BA pairs each, warm 4s measure 7s\n'
} > "$root/results/environment.log" 2>&1
test "$(uname -m)" = arm64
git init -q source
git -C source fetch --depth=1 https://github.com/netty/netty.git "$base"
git -C source checkout --detach FETCH_HEAD
test "$(git -C source rev-parse HEAD)" = "$base"
(cd source; ./mvnw -B -ntp -DskipTests -pl transport -am compile)
src="$root/source"
cp="$src/common/target/classes:$src/buffer/target/classes:$src/resolver/target/classes:$src/transport/target/classes"
for artifact in jctools-core jctools-core-jdk11; do
  jar="$HOME/.m2/repository/org/jctools/$artifact/4.0.7/$artifact-4.0.7.jar"
  test -f "$jar"
  cp="$cp:$jar"
done
path=transport/src/main/java/io/netty/channel/nio/NioIoHandler.java
git -C source apply --check --include="$path" "$root/candidate.patch"
git -C source apply --include="$path" "$root/candidate.patch"
mkdir -p candidate benchmark
javac --release 8 -encoding UTF-8 -cp "$cp" -d candidate "$src/$path"
javac --release 21 -encoding UTF-8 -cp "$cp" -d benchmark HealthyNioRoundTrip.java
run_case() {
  local conns="$1" pair="$2" kind="$3"
  local runtime="$root/benchmark:$cp"
  if [ "$kind" = candidate ]; then runtime="$root/candidate:$runtime"; fi
  local log="$root/results/$conns-$pair-$kind.log"
  printf '%s %s %s %s\n' "$(date -u +%FT%TZ)" "$conns" "$pair" "$kind" | tee -a results/order.log
  java -Xms512m -Xmx512m -cp "$runtime" HealthyNioRoundTrip 4 "$conns" 2000 4 7 > "$log" 2>&1
  grep -E '^selector=.*KQueueSelectorProvider .*echoed_payloads=verified' "$log"
}
for conns in 4 20; do
  for pair in 1 2 3 4 5; do
    if (( pair % 2 )); then
      run_case "$conns" "$pair" baseline
      run_case "$conns" "$pair" candidate
    else
      run_case "$conns" "$pair" candidate
      run_case "$conns" "$pair" baseline
    fi
  done
done
python3 - <<'PY' | tee results/summary.log
from pathlib import Path
from re import search
from statistics import median
for connections in (4, 20):
    for key in ("server_cpu_us_per_request", "requests_s"):
        def read(pair, variant):
            s = Path(f"results/{connections}-{pair}-{variant}.log").read_text()
            return float(search(r"\b" + key + r"=([0-9.]+)", s)[1])
        deltas = [100 * (read(i, "candidate") / read(i, "baseline") - 1) for i in range(1, 6)]
        print(f"{connections} connections {key}: median {median(deltas):+.2f}%; all pairs {deltas}")
PY

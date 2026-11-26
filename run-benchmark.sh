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
  printf '4 server loops, 2us/hop, 4 and 20 connections, six balanced triplets each, warm 4s measure 7s\n'
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
cat > load-guard.patch <<'LOAD_GUARD_PATCH'
--- a/transport/src/main/java/io/netty/channel/nio/NioIoHandler.java
+++ b/transport/src/main/java/io/netty/channel/nio/NioIoHandler.java
@@ -613,7 +613,8 @@
 
     @Override
     public void wakeup() {
-        if (!executor.isExecutorThread(Thread.currentThread()) && wakenUp.compareAndSet(false, true)) {
+        if (!executor.isExecutorThread(Thread.currentThread()) && !wakenUp.get()
+                && wakenUp.compareAndSet(false, true)) {
             selector.wakeup();
         }
     }
LOAD_GUARD_PATCH
mkdir -p baseline guard candidate benchmark
javac --release 8 -encoding UTF-8 -cp "$cp" -d baseline "$src/$path"
git -C source apply --check "$root/load-guard.patch"
git -C source apply "$root/load-guard.patch"
javac --release 8 -encoding UTF-8 -cp "$cp" -d guard "$src/$path"
git -C source apply -R "$root/load-guard.patch"
git -C source apply --check --include="$path" "$root/candidate.patch"
git -C source apply --include="$path" "$root/candidate.patch"
javac --release 8 -encoding UTF-8 -cp "$cp" -d candidate "$src/$path"
javac --release 21 -encoding UTF-8 -cp "$cp" -d benchmark HealthyNioRoundTrip.java
shasum -a 256 load-guard.patch candidate.patch HealthyNioRoundTrip.java >> results/environment.log
for conns in 4 20; do
  round=0
  for order in 'baseline guard candidate' 'guard candidate baseline' 'candidate baseline guard' 'baseline candidate guard' 'guard baseline candidate' 'candidate guard baseline'; do
    round=$((round+1))
    for variant in $order; do
      printf '%s %s %s %s\n' "$(date -u +%FT%TZ)" "$conns" "$round" "$variant" | tee -a results/order.log
      log="results/$conns-$round-$variant.log"
      java -Xms512m -Xmx512m -cp "$root/$variant:$root/benchmark:$cp" HealthyNioRoundTrip 4 "$conns" 2000 4 7 > "$log" 2>&1
      grep -E '^selector=.*KQueueSelectorProvider .*echoed_payloads=verified' "$log"
    done
  done
done
python3 - results <<'SUMMARIZE_TRIPLETS' | tee results/summary.log
import re
import sys
from pathlib import Path
from statistics import median
logs = Path(sys.argv[1])
for conns in (4, 20):
    for metric in ("server_cpu_us_per_request", "requests_s"):
        values = {}
        for variant in ("baseline", "guard", "candidate"):
            values[variant] = [
                float(re.search(fr"\b{metric}=([0-9.]+)", (logs / f"{conns}-{i}-{variant}.log").read_text())[1])
                for i in range(1, 7)
            ]
        print(f"{conns} connections, {metric}")
        for variant, samples in values.items():
            print(f"  {variant}: median {median(samples):.4f}; samples {samples}")
        for x, y in (("guard", "baseline"), ("candidate", "baseline"), ("candidate", "guard")):
            deltas = [(a/b-1)*100 for a,b in zip(values[x],values[y])]
            print(f"  {x}/{y}: paired median {median(deltas):+.2f}%; pairs {[round(d, 2) for d in deltas]}")
SUMMARIZE_TRIPLETS

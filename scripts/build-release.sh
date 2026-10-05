#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
output_dir=${1:-"$project_root/dist"}
mkdir -p -- "$output_dir"
output_dir=$(cd -- "$output_dir" && pwd)
cd -- "$project_root"

for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do
  target_os=${target%-*}
  target_arch=${target#*-}
  CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" \
    go build -trimpath -ldflags='-s -w' -o "$output_dir/eventbus-$target" .
done

cd -- "$output_dir"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum eventbus-linux-amd64 eventbus-linux-arm64 eventbus-darwin-amd64 eventbus-darwin-arm64 > SHA256SUMS
else
  shasum -a 256 eventbus-linux-amd64 eventbus-linux-arm64 eventbus-darwin-amd64 eventbus-darwin-arm64 > SHA256SUMS
fi
printf 'Built EventBus release artifacts in %s\n' "$output_dir"

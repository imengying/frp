#!/usr/bin/env bash
#
# 打包 frps / frpc 的分发压缩包。
#
# 用法:
#   scripts/package.sh <组件> <版本> <target> [输出目录]
#
# 示例:
#   scripts/package.sh frps v1.0.0 x86_64-unknown-linux-musl dist
#   -> dist/frps_v1.0.0_x86_64-unknown-linux-musl.tar.gz
#
# Windows 目标产出 .zip，其他产出 .tar.gz。标准输出打印产物路径。
#
# 这个脚本同时被 .github/workflows/release.yml 和 ci.yml 使用：
# release 用它产出真正的发布包，CI 用它做冒烟测试（每次 push 都验证一遍
# 包内文件是否完整、两个组件有没有混进对方的东西）。

set -euo pipefail

comp="${1:?用法: package.sh <frps|frpc> <版本> <target> [输出目录]}"
version="${2:?缺少版本号，例如 v1.0.0}"
target="${3:?缺少 target，例如 x86_64-unknown-linux-musl}"
outdir="${4:-dist}"

case "$comp" in
	frps | frpc) ;;
	*)
		echo "不支持的组件: $comp（只能是 frps 或 frpc）" >&2
		exit 1
		;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

binary="target/$target/release/$comp"
if [[ "$target" == *windows* ]]; then
	binary="$binary.exe"
fi

if [[ ! -f "$binary" ]]; then
	echo "找不到构建产物 $binary" >&2
	echo "请先执行: cargo build --release --target $target" >&2
	exit 1
fi

name="${comp}_${version}_${target}"
stage="$outdir/$name"

rm -rf "$stage"
mkdir -p "$stage"
cp "$binary" "$stage/"
cp "${comp}.toml.example" README.md "$stage/"

if [[ "$target" == *windows* ]]; then
	# Windows runner 自带 7z；Linux/macOS 上优先用 zip
	if command -v 7z >/dev/null 2>&1; then
		(cd "$outdir" && 7z a -bso0 -bsp0 "$name.zip" "$name" >/dev/null)
	else
		(cd "$outdir" && zip -qr "$name.zip" "$name")
	fi
	artifact="$outdir/$name.zip"
else
	(cd "$outdir" && tar czf "$name.tar.gz" "$name")
	artifact="$outdir/$name.tar.gz"
fi

# 只保留压缩包，解包目录留在原地会让下游的上传/校验步骤出错
rm -rf "$stage"

echo "$artifact"

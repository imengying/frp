#!/usr/bin/env bash
#
# 打包 frps / frpc 的分发压缩包。
#
# 用法:
#   scripts/package.sh <组件> <target> [输出目录]
#
# 示例:
#   scripts/package.sh frps x86_64-unknown-linux-musl dist
#   -> dist/frps_x86_64-linux-musl.tar.gz，包内就是 frps 与 frps.toml 两个文件
#
# 几条约定：
#   - 包名不带版本号：每个版本有各自的 release 与下载链接，文件名里再写一遍
#     版本号只是重复；也不带 target 里的 `unknown`（rustc 对无厂商目标的占位段）
#   - 包内只有二进制与配套配置，且不套一层目录，解压出来直接就是文件
#   - 配置文件与二进制同名（frps 配 frps.toml），放在一起即可直接运行
#
# 这个脚本同时被 .github/workflows/release.yml 和 ci.yml 使用：
# release 用它产出真正的发布包，CI 用它做冒烟测试（每次 push 都验证一遍
# 包内文件是否完整、两个组件有没有混进对方的东西）。

set -euo pipefail

comp="${1:?用法: package.sh <frps|frpc> <target> [输出目录]}"
target="${2:?缺少 target，例如 x86_64-unknown-linux-musl}"
outdir="${3:-dist}"

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
if [[ ! -f "$binary" ]]; then
	echo "找不到构建产物 $binary" >&2
	echo "请先执行: cargo build --release --target $target" >&2
	exit 1
fi

# x86_64-unknown-linux-musl -> x86_64-linux-musl
short_target="${target//-unknown-/-}"
name="${comp}_${short_target}"

mkdir -p "$outdir"
outdir_abs="$(cd "$outdir" && pwd)"

# 在临时目录里备料：解包目录留在输出去会让下游的上传/校验步骤数错文件。
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT

install -m755 "$binary" "$stage/$comp"
install -m644 "${comp}.toml.example" "$stage/$comp.toml"

# 先切到 stage，再把**文件名**逐个列给 tar。
# 不能写成 `tar czf x.tar.gz .`：那样包内路径会带上 `./` 前缀。
(cd "$stage" && tar czf "$outdir_abs/$name.tar.gz" "$comp" "$comp.toml")

echo "$outdir_abs/$name.tar.gz"

#!/usr/bin/env bash
#
# 校验一个发布包的结构与可用性。
#
# 用法:
#   scripts/verify-package.sh <包路径> <组件> [本机二进制目录]
#
# 例:
#   scripts/verify-package.sh dist/frps_x86_64-linux-musl.tar.gz frps
#
# 第三个参数是「与本机架构相同」的同组件二进制所在目录，用于校验包内配置。
# 目标是交叉架构时（aarch64/armv7 的包在 x86_64 机器上跑不起来），
# 用它来跑配置校验——配置是文本，与架构无关，仍然应该被验证。
# 传了这个参数就表示「包内二进制与当前机器架构不同」。
#
# 这个脚本被 ci.yml 使用。抽出来单独成文件是为了能在本地直接跑，
# 不必推上去看 CI 结果——CI 里的 bash 还多一层 YAML 转义，容易写错。

set -euo pipefail

pkg="${1:?用法: verify-package.sh <包路径> <组件> [本机二进制目录]}"
comp="${2:?缺少组件名（frps 或 frpc）}"
native_dir="${3:-}"

case "$comp" in
	frps | frpc) ;;
	*)
		echo "不支持的组件: $comp（只能是 frps 或 frpc）" >&2
		exit 1
		;;
esac

[ -f "$pkg" ] || {
	echo "找不到包 $pkg" >&2
	exit 1
}

echo "== $pkg =="

# 先把清单读进变量再判断。
# 不能写成 `tar tzf ... | grep -q`：grep -q 命中后立刻退出，tar 收到
# SIGPIPE 返回非 0，在 pipefail 下整条管道算失败，判断会静默失效。
listing=$(tar tzf "$pkg")
echo "$listing"

# 包里只有二进制与配套配置，且必须是平铺的（不能套一层目录），
# 否则用户解压后还得再 cd 进去，也可能把上级目录搞乱。
# 不能用 $'...' 拼期望值：那是 ANSI-C quoting，变量不会展开。
expected=$(printf '%s\n%s.toml' "$comp" "$comp")
if [ "$listing" != "$expected" ]; then
	echo "$pkg 内容不合预期，应恰好是 ${comp} 与 ${comp}.toml 两个平铺文件"
	echo "实际："
	printf '%s\n' "$listing"
	exit 1
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
tar xzf "$pkg" -C "$work"

[ -x "$work/$comp" ] || {
	echo "解压后 ${comp} 没有可执行权限"
	exit 1
}

checker_bin="$work/$comp"
if [ -n "$native_dir" ]; then
	# 交叉目标：包内二进制跑不起来（架构不同），改用本机同组件的二进制
	# 校验包内配置。
	checker_bin="$native_dir/$comp"
	[ -x "$checker_bin" ] || {
		echo "找不到用于校验配置的本机二进制 $checker_bin"
		exit 1
	}
	echo "（交叉架构：用本机 $checker_bin 校验包内配置）"
else
	"$work/$comp" --version >/dev/null || {
		echo "${comp} --version 执行失败"
		exit 1
	}
fi

"$checker_bin" -c "$work/${comp}.toml" --check >/dev/null || {
	echo "包内 ${comp}.toml 无法通过配置校验"
	exit 1
}

echo "✅ $pkg 检查通过"

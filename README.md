# agent-bus

多端 / 多 CLI agent 协作总线。解决"服务器 A 一个会话、电脑 B 一个会话、同项目几个会话、还可能是不同 CLI"之间无法配合的问题。

不改你的工作流：代码照常走 git，agent-bus 只管**协调**——谁拥有什么工作、谁在改什么文件、谁跟谁说好了什么、做了一半的工作怎么交给别人。单文件、Python 3.8+ 标准库、零依赖。

## 核心抽象

```
Peer      身份 / rank 权限次序 / 心跳
Claim     工作声明："这件事此刻归谁管"（运行时协调状态，不是任务管理）
Lock      资源排他：目录 / 文件 / 行区域，租约自动续期，--wait 排队
Message   公聊 + 私聊（阻塞型：回合制协商 → 高权限方裁决）
Handoff   两阶段工作交接：claim + 锁 + 上下文 capsule 原子转移
Takeover  对方掉线时的被动接管（从事件流合成事故现场 capsule）
Change    改动小历史（类似 git log 的"谁改了什么"）
Board     共享黑板（结论性信息，决策自动归档）
EventLog  events.jsonl：一切状态变更的事实源，可审计可回溯
```

边界声明：agent-bus 是 **coordination layer**，不是任务管理器（那是 beads / Jira 的地盘）、不是 orchestrator（不 spawn、不调度 agent）、不替代 git（代码内容永远走 git）。

## 快速开始

```bash
# 第一个端（主机）：在项目根目录启动 hub
python3 bus.py serve            # 打印 hub 地址 http://<ip>:8977#<token>

# 每个端加入（同项目多会话务必各起 --name）
python3 bus.py join --hub 'http://<ip>:8977#<token>' --name A --cli kimi

# 典型工作循环
python3 bus.py claim auth-v2 --note "迁移登录接口" --scope src/auth/
python3 bus.py lock src/auth/ --note "改认证"
# ... 干活 ...
python3 bus.py done "登录接口迁到 /v2/auth" --files src/auth/api.ts
python3 bus.py unlock --all && python3 bus.py sync

# 收工交接给另一台机器上的 B
python3 bus.py handoff B auth-v2 --state "实现80%" --next "改 tests" --patch
```

## 多机互联（含 Tailscale 自动支持）

- **没装 Tailscale？** `bus net setup` 一条命令引导安装：macOS 走 Homebrew、Linux 走官方脚本，自动装好后启动守护进程，只在两个绕不开的人工点停下——sudo 密码、浏览器登录 tailnet 授权。`bus net status` 随时查看状态。
- `bus serve` 自动探测 **Tailscale IPv4**（`tailscale ip -4`）：检测到就把它作为首选广告地址，未检测到回退局域网 IP 并提示安装。所有候选地址（Tailscale / 局域网 / 本机）都会写进 `.bus/hub.json` 的 `alt_urls`。
- `bus join`（不带 `--hub`，读 hub.json）会**逐个探测候选地址、自动选路**，主地址不通自动换备选——你在公司局域网 join 过一次，回家切到 Tailscale 也无需改任何配置。
- 也就是说：多台机器装好 Tailscale 后，整个互联是**零配置**的，而且流量走 WireGuard 加密，顺带解决明文 HTTP 的问题。
- 没有 Tailscale 的可选方案：Cloudflare Tunnel（`cloudflared tunnel --url http://localhost:8977`，零对端安装、白送 TLS）、SSH 反向隧道（`autossh -R 8977:localhost:8977 user@vps`）。

## 命令速查

| 命令 | 作用 |
|---|---|
| `serve [--port 8977] [--dir .bus]` | 启动 hub |
| `join --hub <url#token> [--name N]` | 加入总线，按顺序分配 rank |
| `sync` | 心跳 + 新公聊/改动/私聊/交接/声明 一屏看完 |
| `claim <名> [--note] [--scope] [--status] [--waiting-on]` | 声明/更新工作归属 |
| `claims [--all]` / `unclaim <名> [--abandoned]` | 查看 / 关闭工作声明 |
| `lock <路径> [-r 起:止] [--ttl 分钟] [--wait]` | 加锁：目录（`/`结尾）/文件/区域；租约；排队 |
| `unlock <路径> \| --all [--force]` | 解锁；force 需更高权限/主机/对方掉线 |
| `locks` / `peers [rm <名字>]` / `status` | 看锁与等待队列 / 各端与权限（主机可移除掉线 peer）/ 全貌 |
| `handoff <对方\|anyone> <claim> [--state] [--blockers] [--next] [--patch]` | 发起交接（两阶段） |
| `capsule <hid>` / `accept <hid>` / `reject <hid>` | 看交接详情 / 接收 / 拒绝 |
| `takeover <claim> --reason "..."` | 接管掉线（或低权限）方的工作 |
| `done "摘要" [--files] [--detail]` | 记录改动（自动附 git commit） |
| `log [-n]` / `events [-n] [--type]` | 改动历史 / 事件流审计 |
| `say all "..."` | 公聊 |
| `say <名字> "..." [--blocking] [--rounds N] [--deadline M]` | 私聊；阻塞型限定回合与时限 |
| `reply / resolve / decide / thread` | 私聊回复 / 共识归档 / 高权限裁决 / 看全文 |
| `board` / `board add <分区> "..."` / `board reset`（主机） | 共享黑板：查看 / 追加 / 重置（先 `bus archive` 归档） |
| `archive <目录>` | 归档黑板/公聊/私聊/改动/声明到本地目录，重置与审计用 |
| `leave` | 离开：释放锁、取消未决交接 |

## 关键语义

- **权限**：rank 0 = 主机；掉线后在线 rank 最小者接任。强制解锁、超时裁决、强制接管都按此次序判权限。
- **锁租约**：默认 15 分钟，持锁者任何操作自动续期；停止续期（掉线）即自动过期，等待队列按序递补。
- **阻塞私聊**：默认 6 回合 / 30 分钟，任一耗尽转"待裁决"，权限高者 `decide` 一锤定音，自动归档黑板「决策记录」。
- **Handoff**：offer 期间锁被保护（不可 unlock/force）；`accept` 时 claim + 锁原子转移；`reject`/超时（30 分钟）自动还原。工作区有未提交改动时必须 `--patch` 打包或先 commit。
- **Takeover**：对方在线→仅更高权限者可强制接管；异常掉线→20 分钟内仅更高权限者/主机，之后任何人；主动 leave→任何人立即可接管。salvage capsule 标记 partial，先核对 git 现场再动手。
- **心跳**：任何命令都算心跳；10 分钟无操作视为掉线。
- **同项目多会话**：`join --name 不同名字`，身份存为 `.bus-peer.<名字>.json`；`BUS_PEER_FILE` 环境变量切换当前会话身份。

## 数据目录（.bus/）

```
state.json     状态快照
events.jsonl   事件流（事实源）：peer.joined / lock.acquired / handoff.accepted / ...
board.md       共享黑板
hub.json       hub 地址与 token（可随 git 同步，供对端自动发现）
changes/       改动详情 md
capsules/      交接携带的 wip patch
```

纯文本，可整个提交进项目 git 做持久化与审计。hub 单点故障时，任一端可用同一数据目录重新 `serve`，其余端改 `--hub` 重新 join。

## 安装为 skill

```bash
mkdir -p ~/.agents/skills/agent-bus
cp bus.py ~/.agents/skills/agent-bus/
cp skill/SKILL.md ~/.agents/skills/agent-bus/
install -m755 bus.py ~/.local/bin/bus   # 可选：让 bus 直接在 PATH 上
```

其他 CLI（Claude Code、Codex 等）同理，把 `skill/SKILL.md` 放进它们的指令/技能目录即可——契约是纯文本，与 CLI 无关。

## Hooks：把纪律变成机制（v0.3）

skill 契约靠 agent 自觉，hooks 把它升级为半强制：**写文件前自动加锁、锁冲突直接拦截这次工具调用、回合结束自动心跳并提醒未处理事项**。

```bash
bus install-hooks claude            # ~/.claude/settings.json
bus install-hooks kimi              # ~/.kimi-code/config.toml
bus install-hooks codex             # ~/.codex/config.toml
bus install-hooks opencode          # ~/.config/opencode/plugins/agent-bus.ts
bus install-hooks pi                # ~/.pi/agent/extensions/agent-bus/index.ts
bus install-hooks all               # 全部
# claude/opencode/pi 支持 --scope project（装到当前项目目录）
```

- 原理：`bus hook <cli>` 从 stdin 读各 CLI 的 hook payload，exit 0 放行 / exit 2 拦截（stderr 即拦截原因，会回灌给模型）。
- 覆盖：Edit/Write 类工具自动锁；Bash 类工具嗅探 `>`、`tee`、`sed -i` 的写入目标做冲突拦截（尽力而为，允许漏判）；Stop/idle 事件自动心跳，有阻塞私聊或待接收交接时拦截收工。
- 已实测：Claude Code 与 Kimi Code 的 payload 协议（exit 2 / stderr / JSON 语义）。Codex 的 hooks 配置按其官方"Claude 风格"文档生成，OpenCode/PI 按官方插件文档生成——这三家请以实际版本实测为准。
- hub 不可达时默认 fail-open（放行）；`BUS_HOOK_ENFORCE=1` 切换为 fail-closed。

## 已知取舍

- 锁与 claim 是 advisory + hook 半强制（bash 改写文件无法 100% 拦截），`events.jsonl` 全量审计兜底。
- 网络为明文 HTTP + token，适用于可信内网/VPN（推荐 Tailscale）；跨公网请走 SSH 转发/Cloudflare Tunnel。
- 死锁检测（A 等 B、B 等 A 成环）暂未实现，等待队列已是 FIFO，环检测在 roadmap。
- 消息/事件流无上限截断（查询只返回最近 N 条），规模大了再做分片与 compaction。

## v0.4 变更

- **健壮性**：hub 对数据文件缺失零崩溃——`board.md`/`changes/`/`capsules/` 按需懒创建（此前 board.md 被外部清理后 `POST /api/board` 直接断连）；handler 兜底捕获 `OSError`/未知异常并返回 5xx JSON（此前崩溃导致客户端只看到连接断开）。
- **运维**：`bus serve` 检测数据目录在系统临时目录（/tmp 等）时打印迁移警告——macOS periodic daily 按 atime 清理 /tmp 超 3 天未访问文件，会静默清掉 `board.md`/`hub.json`/`bus.py`。
- **新命令**：`bus board reset`（主机权限，重置黑板为初始模板）、`bus archive <目录>`（归档黑板/公聊/私聊/改动/声明，重置前必做）、`bus peers rm <名字>`（主机权限，移除掉线 peer 并释放其锁）。
- **join 去重**：同名 peer 在线时拒绝（409）；同名离线时自动回收旧条目，避免重 join 产生重复残留（此前会永久留两条同名 peer）。

## Roadmap

- v0.1 ✅ 锁/私聊/裁决/改动历史/黑板
- v0.2 ✅ Event Log、Claim、Handoff/Takeover、锁租约与等待队列
- v0.3 ✅ CLI hooks：自动锁/冲突拦截/自动心跳，支持 Claude Code、Kimi Code、Codex、OpenCode、PI
- v0.4 ✅ 健壮性（数据文件懒创建 + handler 兜底 5xx）、board reset / archive / peers rm、join 去重、临时目录警告
- v0.5 规划：结构化消息（--type finding/blocker + ACK）、`bus assign` 任务下发、scope 命名空间、decision versioning（supersede/revoke）、死锁检测、`sync --wait` long-polling
- 更远的：capability registry、agentd（watch 事件流自动起 headless agent）、MCP/ACP adapter、hub replication

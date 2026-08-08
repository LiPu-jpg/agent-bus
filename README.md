# agent-bus

多端 / 多 CLI agent 协作总线。解决"服务器 A 一个会话、电脑 B 一个会话、同项目几个会话、还可能是不同 CLI"之间无法配合的问题。

不改你的工作流：代码照常走 git，agent-bus 只管**协调**——锁、消息、改动历史、共享黑板。单文件、Python 3.8+ 标准库、零依赖。

## 架构

- **hub**：`bus.py serve` 启动的小型 HTTP 服务，跑在任一端可达的机器上（通常就是第一个会话所在的机器）。所有状态存在一个数据目录里（默认 `.bus/`）：`state.json` + `board.md` + `changes/*.md`，全是纯文本，可以直接提交进项目 git 做持久化和审计。
- **client**：同一个 `bus.py` 的子命令，agent（或人）在项目目录里直接调用。
- **skill**：`skill/SKILL.md` 是行为契约，装进各 CLI 的 skills 目录后，任何 agent 都遵守同一套纪律。

## 快速开始

```bash
# 第一个端（主机）：在项目根目录启动 hub
python3 bus.py serve            # 打印 hub 地址 http://<ip>:8977#<token>

# 每个端加入（同项目多会话务必各起 --name）
python3 bus.py join --hub 'http://<ip>:8977#<token>' --name A --cli kimi

# 日常三件套的其余部分
python3 bus.py sync                                  # 心跳 + 收消息
python3 bus.py lock src/a.ts --note "改登录接口"      # 改文件前先锁
python3 bus.py done "登录接口迁到 /v2/auth" --files src/a.ts
python3 bus.py unlock --all
```

## 命令速查

| 命令 | 作用 |
|---|---|
| `serve [--port 8977] [--dir .bus]` | 启动 hub |
| `join --hub <url#token> [--name N]` | 加入总线，按顺序分配 rank |
| `sync` | 心跳 + 新公聊 + 新改动 + 私聊（阻塞优先）+ 待裁决 |
| `lock <路径> [-r 起:止] [--note]` | 目录锁（`/`结尾）/ 文件锁 / 区域锁 |
| `unlock <路径> \| --all [--force]` | 解锁；force 需更高权限/主机/对方掉线 |
| `locks` / `peers` / `status` | 看锁 / 看各端与权限 / 看全貌 |
| `done "摘要" [--files] [--detail]` | 记录改动（自动附 git commit），别人 sync 可见 |
| `log [-n]` | 改动历史 |
| `say all "..."` | 公聊 |
| `say <名字> "..." [--blocking] [--rounds N] [--deadline M]` | 私聊；阻塞型限定回合与时限 |
| `reply <tid> "..."` | 回复私聊 |
| `resolve <tid> "共识"` | 双方达成一致，归档黑板 |
| `decide <tid> "最终方案"` | 高权限方/主机在谈不拢时裁决定案 |
| `thread <tid>` | 看私聊全文 |
| `board` / `board add <分区> "..."` | 共享黑板 |
| `leave` | 离开并释放自己的锁 |

## 关键语义

- **权限**：rank 0 = 主机，权限最高；掉线后在线 rank 最小者接任。强制解锁、超时裁决都按此次序裁决权限。
- **阻塞私聊**：默认 6 回合 / 30 分钟，任一耗尽即转"待裁决"，由双方中权限高者（或主机）`decide` 总结定案，结果自动写入黑板「决策记录」。
- **心跳**：任何命令都算心跳；10 分钟无心跳视为掉线，其锁可被任何人 `--force` 解除。
- **同项目多会话**：`join --name 不同名字`，身份存为 `.bus-peer.<名字>.json`；用 `BUS_PEER_FILE` 环境变量切换当前会话身份。

## 安装为 skill

```bash
mkdir -p ~/.agents/skills/agent-bus
cp bus.py ~/.agents/skills/agent-bus/
cp skill/SKILL.md ~/.agents/skills/agent-bus/
install -m755 bus.py ~/.local/bin/bus   # 可选：让 bus 直接在 PATH 上
```

其他 CLI（Claude Code、Codex 等）同理，把 `skill/SKILL.md` 放进它们的指令/技能目录即可——契约是纯文本，与 CLI 无关。

## 已知取舍（MVP）

- 锁是 advisory（约定式），靠 skill 纪律保证；跨 CLI 无法做强制锁，审计靠 `state.json` 全量可追溯。
- hub 单点：主机所在机器挂了，任一端可用同一数据目录重新 `serve`（数据目录建议随项目 git 同步），其余端改 `--hub` 重新 join。
- 消息/改动流目前无上限截断（`status` 只返回最近 50 条），规模大了再做分片。

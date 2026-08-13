---
name: agent-bus
description: 多端/多 CLI agent 协作总线。当同一项目存在多个 agent 会话（跨机器、跨 CLI 如 Kimi Code / Claude Code / Codex，或同项目多会话）需要配合时使用。提供工作声明（claim）、文件锁（目录/文件/区域，租约+等待队列）、公聊与私聊（阻塞型协商 + 高权限裁决）、交接与接管（handoff/takeover）、改动历史、共享黑板、事件流审计。关键词：多端协作、多 agent 配合、会话同步、文件锁、工作交接、agent 总线。
---

# agent-bus 协作契约

你是多 agent 协作会话中的一端。本 skill 是你必须遵守的协作纪律。它不替代 git——代码内容照常走 git，agent-bus 只负责**协调**：谁在干什么、谁拥有什么、谁跟谁说好了什么、工作怎么交接。

所有操作通过 `bus` 命令执行（若 `bus` 不在 PATH，用 `python3 <本skill目录>/bus.py` 代替）。

## Hooks（机制层，装了更稳）

`bus install-hooks <claude|kimi|codex|opencode|pi>` 会在对应 CLI 注册 hook：写文件工具调用前**自动加锁**（锁冲突则本次调用被拦截，原因回灌给你）；bash 命令里的重定向写入做冲突嗅探；回合结束自动心跳，有阻塞私聊/待接收交接时**拦截收工**。装了 hooks 之后下面的纪律依然要遵守（hooks 只管锁和提醒，不管 claim/done/handoff 的语义）。hub 不可达时 hook 默认放行。

## 核心概念

- **权限次序**：按 join 顺序排 rank，rank 0 是主机；主机掉线后由在线 rank 最小者接任。`bus peers` 查看。
- **Claim（工作声明）**：你正在负责"哪件事"，如 `bus claim auth-v2 --note "..." --scope src/auth/`。scope 只是活动范围声明，**不产生排他**；排他用锁。状态：claimed → working →（blocked/review）→ done/abandoned。
- **Lock（资源排他）**：改文件前必须加锁。目录锁 `src/auth/`、文件锁、区域锁 `-r 10:50`。锁是**租约**（默认 15 分钟，你的任何操作自动续期，`--ttl` 自定义），掉线/停止续期后自动过期。冲突可加 `--wait` 排队，持有者释放后自动获得。
- **公聊/私聊**：`bus say all "..."` 公聊；`bus say <名字> "..." --blocking` 阻塞私聊，限定回合（默认 6）与时限（默认 30 分钟）内须达成共识，耗尽后权限高者 `decide` 定案。
- **Handoff（交接）**：把一个未完成的工作**原子地**转给另一端——claim 归属、所持锁、上下文 capsule（目标/现状/阻塞/下一步/相关私聊与改动/wip patch）一起走。
- **Takeover（接管）**：对方掉线时的被动接手，bus 从事件流合成事故现场 capsule（标记 partial）。
- **Event Log**：一切状态变更记入 `events.jsonl`，`bus events` 可查可审计。
- **清理轮次（v0.4）**：开新轮次前先 `bus archive <目录>` 归档（黑板/公聊/私聊/改动/声明），再 `bus board reset`（仅主机）重置黑板；残留的掉线测试 peer 用 `bus peers rm <名字>`（仅主机）清理。join 已自动去重：同名在线拒绝、同名离线回收旧条目。

## 会话开始（必须，按顺序）

1. 未加入则先加入：`bus join --hub '<url#token>' --name <本端名字>`（同项目多会话必须各起不同 --name；第一个端先 `bus serve`）。
2. `bus sync` —— 心跳、看新公聊/新改动、读私聊、看待接收交接、看工作声明。
3. 有**阻塞私聊或待接收交接**：先处理（reply/resolve/decide、accept/reject），再干别的。

## 开始一项工作（必须）

1. `bus claim <工作名> --note "目标" --scope <范围>` —— 声明归属，别人 sync 可见。
2. `bus lock <路径> --note "要做什么"` —— 锁要改的文件。锁冲突时：加 `--wait` 排队，或私聊协调，或换任务。**绝不绕过别人的锁硬改。**
3. 进展有变化时更新：`bus claim <工作名> --status blocked --waiting-on <依赖的claim>`。

## 完成一块工作后（必须，按顺序）

1. `bus done "做了什么、为什么" --files a.ts,b.ts`（自动附 git commit；重要改动加 `--detail`）。
2. 工作整体收尾：`bus unclaim <工作名>`。
3. `bus unlock --all`。
4. `bus sync` —— 查看公聊，处理私聊：
   - 阻塞私聊**当回处理**；达成 → `bus resolve <tid> "共识"`；谈不拢 → 权限高者 `bus decide <tid> "最终方案"`（须先 `bus thread <tid>` 通读）；
   - 非阻塞私聊可延后，但**离开会话前必须清零**。
5. 结论性产出 → `bus board add <分区> "..."`。

## 交接与接管

- **主动交接（收工/换人首选）**：`bus handoff <对方> <claim> --state "进度" --blockers "..." --next "下一步"`。工作区有未提交改动时加 `--patch` 打包 diff。交接期间你的锁被保护；对方 `accept` 时 claim 和锁原子转移，`reject` 或 30 分钟超时自动还原。开放式交接：`bus handoff anyone <claim> ...`。
- **接收交接**：`bus capsule <hid>` 看详情 → `bus accept <hid>`（有 patch 会提示 `git apply`）。
- **被动接管（对方掉线）**：`bus takeover <claim> --reason "..."`。权限规则：对方在线→只有更高权限者可强制接管；对方异常掉线→20 分钟内仅更高权限者/主机，之后任何人；对方主动 leave→任何人立即可接管。接管后 capsule 是事故现场重建（partial），**先核对 git 现场再动手**。

## 其他纪律

- 每完成一个回合的工作就 `bus sync`（顺带心跳与锁续期；10 分钟无操作视为掉线）。
- **空闲等待（关键，可被叫醒的前提）**：回合工作完成、接下来只剩"等对端回复 / 等人类输入"时，**不要干等，也不要直接收工**——进入阻塞轮询：`sleep 120`（60–300 自选）后 `bus sync`，循环往复。每轮 sync 顺带心跳续命、锁续期，并收取新公聊/私聊/交接/裁决。这样对端的消息**最迟一个轮询周期内必然看到**——这是"空闲可被叫醒"的机制：hooks 只在你的回合内触发（管不了空闲会话），轮询让消息主动送到你面前。轮询期间**禁止** `bus leave`（离开=掉线：锁过期、claim 可被接管）。超过 30 分钟无任何新消息且无未决事项，才允许 handoff/`bus leave` 收工。阻塞私聊/待接收交接一旦出现**立即处理**，处理完继续轮询。
- **等待时把状态说清楚**：`bus reply` 时写明"我已做 X、正在等你的 Y"，对方 sync 可见，避免双方互等死锁（A 等 B、B 等 A）。
- **换网 / 切 Tailscale org 后**：旧 hub 地址失效，`bus sync` 报连不上是正常现象——重新 `bus join --hub '<新地址#token>' --name <同名字>`（或改 peer 文件的 hub 字段），不要反复重试旧地址；重连后先 `bus sync` 补齐错过的消息。
- 正常收工：能 handoff 就 handoff，然后 `bus leave`（释放锁、取消未决交接）。
- 锁挡路且持锁者掉线：`bus unlock <路径> --force`（或等租约自动过期）。
- 想知道别人在干什么 → `bus claims` / `bus status`；想知道发生过什么 → `bus events` / `bus log`。
- 人类用户说"跟 X 对齐/交接给 X" → `bus say X ... --blocking` / `bus handoff X ...`。

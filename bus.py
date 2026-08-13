#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
agent-bus — 多端 agent 协作总线（单文件、零依赖，Python 3.8+ 标准库）

v0.2 核心抽象：
  Peer（身份/权限/心跳）  Claim（工作声明）   Lock（目录/文件/区域 + 租约 + 等待队列）
  Message（公聊/私聊/阻塞协商/裁决）  Handoff（两阶段交接 + 死后接管）
  Change（改动小历史）  Board（共享黑板）  Event Log（全部状态变更的事实源）

数据目录（默认 .bus/）：
  state.json    状态快照（缓存）
  events.jsonl  事件流（事实源，一切写操作追加）
  board.md      共享黑板
  changes/      改动详情
  capsules/     交接 capsule 的 wip patch
"""
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

VERSION = "0.4.2"
TTL = 600                     # peer 心跳有效期（秒），超时视为掉线
LOCK_LEASE = 900              # 锁租约（秒），持锁者任何操作自动续期
LOCK_LEASE_MAX = 8 * 3600     # 自定义 --ttl 上限
HANDOFF_TTL = 1800            # 交接 offer 有效期（秒）
DEFAULT_PORT = 8977
DEFAULT_ROUNDS = 6            # 阻塞私聊默认协商回合数
DEFAULT_DEADLINE_MIN = 30     # 阻塞私聊默认时限（分钟）
PEER_FILE = ".bus-peer.json"  # 项目目录里的本端身份文件


def now():
    return time.time()


def fmt_time(t=None):
    return time.strftime("%m-%d %H:%M:%S", time.localtime(t or now()))


def short_id():
    return uuid.uuid4().hex[:8]


def norm_path(p):
    p = p.strip()
    while p.startswith("./"):
        p = p[2:]
    return p


def is_dir_path(p):
    return p.endswith("/")


def paths_conflict(a, b):
    """目录锁与其子路径互斥；同一路径互斥。"""
    if a == b:
        return True
    if is_dir_path(a) and (b == a[:-1] or b.startswith(a)):
        return True
    if is_dir_path(b) and (a == b[:-1] or a.startswith(b)):
        return True
    return False


def region_overlap(r1, r2):
    """region 形如 '10:50'；任一方无 region 视为整文件，必冲突。"""
    if not r1 or not r2:
        return True
    a1, b1 = (int(x) for x in r1.split(":"))
    a2, b2 = (int(x) for x in r2.split(":"))
    return a1 <= b2 and a2 <= b1


def lock_label(l):
    return l["path"] + (f"({l['region']})" if l.get("region") else "")


class BusError(Exception):
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code
        self.msg = msg


# ==================== 服务端：状态与业务逻辑 ====================

class Bus:
    CLAIM_STATUSES = ("claimed", "working", "blocked", "review", "done", "abandoned")

    def __init__(self, datadir):
        self.dir = os.path.abspath(datadir)
        os.makedirs(os.path.join(self.dir, "changes"), exist_ok=True)
        os.makedirs(os.path.join(self.dir, "capsules"), exist_ok=True)
        self.state_path = os.path.join(self.dir, "state.json")
        self.events_path = os.path.join(self.dir, "events.jsonl")
        self.board_path = os.path.join(self.dir, "board.md")
        self.mu = threading.RLock()
        if os.path.exists(self.state_path):
            with open(self.state_path, encoding="utf-8") as f:
                self.state = json.load(f)
        else:
            self.state = {
                "token": uuid.uuid4().hex,
                "peer_seq": 0, "msg_seq": 0, "change_seq": 0, "thread_seq": 0,
                "peers": {}, "locks": {}, "threads": {},
                "inbox": {}, "public": [], "changes": [],
            }
        # 向后兼容：补充 v0.2 新增字段
        self.state.setdefault("event_seq", 0)
        self.state.setdefault("handoff_seq", 0)
        self.state.setdefault("claims", {})
        self.state.setdefault("handoffs", {})
        self.state.setdefault("waiters", [])
        self.token = self.state["token"]
        if not os.path.exists(self.board_path):
            with open(self.board_path, "w", encoding="utf-8") as f:
                f.write("# 共享黑板\n\n> 所有端共同维护。结论性内容写这里，过程讨论去公聊/私聊。\n")

    def save(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.state_path)

    def _emit(self, etype, **data):
        """一切状态变更写事件流（调用方须持锁）。"""
        self.state["event_seq"] += 1
        ev = {"seq": self.state["event_seq"], "ts": now(), "type": etype, **data}
        with open(self.events_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    # ---- peer ----

    def alive(self, p):
        return now() - p["last_seen"] < TTL

    def live_peers(self):
        return sorted((p for p in self.state["peers"].values() if self.alive(p)),
                      key=lambda p: p["rank"])

    def host_peer(self):
        ps = self.live_peers()
        return ps[0] if ps else None

    def get_peer(self, pid):
        p = self.state["peers"].get(pid)
        if not p:
            raise BusError(404, f"未知 peer: {pid}（先 join）")
        t = now()
        p["last_seen"] = t  # 任何操作都算一次心跳
        with self.mu:
            for l in self.state["locks"].values():
                if l["owner"] == pid:  # 顺带续租
                    l["expires"] = t + l.get("lease", LOCK_LEASE)
        return p

    def peer_by_name(self, name):
        for p in self.state["peers"].values():
            if p["name"] == name or p["id"] == name:
                return p
        return None

    def join(self, name, host, cli):
        with self.mu:
            if name:
                dup = next((p for p in self.state["peers"].values()
                            if p["name"] == name), None)
                if dup:
                    if self.alive(dup):
                        raise BusError(
                            409, f"同名 peer「{name}」在线（rank{dup['rank']}），"
                                 f"请换 --name 或先与其协调")
                    # 离线同名：回收旧条目再入队，避免重复残留
                    del self.state["peers"][dup["id"]]
                    self._emit("peer.left", peer=name, released_locks=0,
                               reason="replaced-on-rejoin")
            pid = short_id()
            rank = self.state["peer_seq"]
            self.state["peer_seq"] += 1
            self.state["peers"][pid] = {
                "id": pid, "name": name or f"agent-{rank}",
                "host": host or "?", "cli": cli or "?",
                "rank": rank, "joined_at": now(), "last_seen": now(),
                "cursors": {"public": self.state["msg_seq"],
                            "changes": self.state["change_seq"]},
            }
            self._emit("peer.joined", peer=name or f"agent-{rank}", rank=rank,
                       host=host or "?", cli=cli or "?")
            self.save()
            return self.state["peers"][pid]

    def leave(self, pid):
        me = self.get_peer(pid)
        with self.mu:
            me["last_seen"] = 0
            # 取消我发出的未决交接，锁的 pending 标记一并解除
            for h in self.state["handoffs"].values():
                if h["status"] == "offered" and h["from"] == pid:
                    h["status"] = "cancelled"
                    c = self.state["claims"].get(h["claim"])
                    if c and c["status"] == "handing_off":
                        c["status"] = "working"
                    self._emit("handoff.cancelled", id=h["id"], claim=h["claim"],
                               actor=me["name"], reason="leave")
            released = [k for k, l in self.state["locks"].items() if l["owner"] == pid]
            for k in released:
                del self.state["locks"][k]
            # 我的等待排队也清掉
            self.state["waiters"] = [w for w in self.state["waiters"] if w["peer"] != pid]
            self._emit("peer.left", peer=me["name"], released_locks=len(released))
            if released:
                self._grant_waiters()
            self.save()
            return len(released)

    # ---- 锁（租约 + 等待队列）----

    def _find_conflict(self, pid, path, region):
        for l in self.state["locks"].values():
            if l["owner"] == pid:
                continue
            if paths_conflict(l["path"], path) and \
                    (l["path"] != path or region_overlap(l["region"], region)):
                return l
        return None

    def lock(self, pid, path, region, note, ttl_min=None, wait=False):
        path = norm_path(path)
        if region and not re.fullmatch(r"\d+:\d+", region):
            raise BusError(400, "region 格式应为 起始行:结束行，如 10:50")
        me = self.get_peer(pid)
        lease = LOCK_LEASE
        if ttl_min:
            lease = max(60, min(int(ttl_min * 60), LOCK_LEASE_MAX))
        with self.mu:
            holder = self._find_conflict(pid, path, region)
            if holder:
                if wait:
                    self.state["waiters"].append({
                        "peer": pid, "peer_name": me["name"], "path": path,
                        "region": region, "note": note or "", "since": now(),
                    })
                    pos = len(self.state["waiters"])
                    self._emit("lock.waiting", path=path, region=region,
                               actor=me["name"], holder=holder["owner_name"])
                    self.save()
                    return {"waiting": True, "position": pos,
                            "holder": holder["owner_name"]}
                pend = (f"，且正包含在交接 {holder['pending']} 中"
                        if holder.get("pending") else "")
                raise BusError(
                    409,
                    f"锁冲突：{lock_label(holder)} 正被 {holder['owner_name']}"
                    f"(rank{holder['owner_rank']}) 持有{pend}"
                    + (f"：{holder['note']}" if holder.get("note") else "")
                    + "（可加 --wait 排队）")
            key = f"{path}|{region or ''}"
            self.state["locks"][key] = {
                "key": key, "path": path, "region": region,
                "owner": pid, "owner_name": me["name"], "owner_rank": me["rank"],
                "note": note or "", "since": now(),
                "lease": lease, "expires": now() + lease,
            }
            self._emit("lock.acquired", path=path, region=region,
                       owner=me["name"], lease=lease)
            self.save()
            return {"waiting": False}

    def _clear_pending(self, hid):
        for l in self.state["locks"].values():
            if l.get("pending") == hid:
                del l["pending"]

    def unlock(self, pid, path, region, force):
        path = norm_path(path)
        me = self.get_peer(pid)
        with self.mu:
            key = f"{path}|{region or ''}"
            l = self.state["locks"].get(key)
            if not l:
                raise BusError(404, f"没有找到锁：{key}")
            if l.get("pending"):
                raise BusError(409, f"该锁包含在交接 {l['pending']} 中，"
                                    f"等待对方 accept，或 `bus reject {l['pending']}` 取消交接")
            if l["owner"] != pid:
                if not force:
                    raise BusError(403, f"锁属于 {l['owner_name']}，确认后加 --force 强制解锁")
                owner = self.state["peers"].get(l["owner"])
                owner_dead = not owner or not self.alive(owner)
                host = self.host_peer()
                is_host = host is not None and host["id"] == pid
                if not (owner_dead or me["rank"] < l["owner_rank"] or is_host):
                    raise BusError(403, "权限不足：只有更高权限者、当前主机、或对方掉线后才能强制解锁")
                self._emit("lock.force_released", path=l["path"], region=l["region"],
                           owner=l["owner_name"], actor=me["name"])
            else:
                self._emit("lock.released", path=l["path"], region=l["region"],
                           owner=me["name"])
            del self.state["locks"][key]
            self._grant_waiters()
            self.save()

    def unlock_all(self, pid):
        self.get_peer(pid)
        with self.mu:
            mine = [k for k, l in self.state["locks"].items()
                    if l["owner"] == pid and not l.get("pending")]
            skipped = sum(1 for l in self.state["locks"].values()
                          if l["owner"] == pid and l.get("pending"))
            for k in mine:
                l = self.state["locks"].pop(k)
                self._emit("lock.released", path=l["path"], region=l["region"],
                           owner=l["owner_name"])
            if mine:
                self._grant_waiters()
            self.save()
            return len(mine), skipped

    def _grant_waiters(self):
        """有锁释放后，按排队顺序授予现在能拿到的 waiter（调用方须持锁）。"""
        for w in list(self.state["waiters"]):
            p = self.state["peers"].get(w["peer"])
            if not p or not self.alive(p):
                self.state["waiters"].remove(w)
                continue
            if self._find_conflict(w["peer"], w["path"], w["region"]):
                continue
            self.state["waiters"].remove(w)
            key = f"{w['path']}|{w['region'] or ''}"
            self.state["locks"][key] = {
                "key": key, "path": w["path"], "region": w["region"],
                "owner": w["peer"], "owner_name": w["peer_name"],
                "owner_rank": p["rank"], "note": w["note"], "since": now(),
                "lease": LOCK_LEASE, "expires": now() + LOCK_LEASE,
            }
            self.state["msg_seq"] += 1
            self.state["inbox"].setdefault(w["peer"], []).append({
                "id": self.state["msg_seq"], "from": "bus", "from_name": "总线",
                "body": f"✅ 你排队等待的锁已获得：{w['path']}"
                        + (f"({w['region']})" if w["region"] else ""),
                "thread": None, "blocking": False, "ts": now(), "read": False,
            })
            self._emit("lock.acquired", path=w["path"], region=w["region"],
                       owner=w["peer_name"], via="wait")

    # ---- 消息 ----

    def _thread(self, tid):
        th = self.state["threads"].get(tid)
        if not th:
            raise BusError(404, f"未知私聊：{tid}")
        return th

    def _append_msg(self, th, sender, body, notify_pid):
        self.state["msg_seq"] += 1
        th["messages"].append({"from": sender["id"], "from_name": sender["name"],
                               "body": body, "ts": now()})
        self.state["inbox"].setdefault(notify_pid, []).append({
            "id": self.state["msg_seq"], "from": sender["id"],
            "from_name": sender["name"], "body": body, "thread": th["id"],
            "blocking": th["blocking"], "ts": now(), "read": False,
        })

    def _other_party(self, th, pid):
        return th["parties"][1] if th["parties"][0] == pid else th["parties"][0]

    def _senior_party(self, th):
        a, b = th["parties"]
        pa = self.state["peers"].get(a, {"rank": 10 ** 9})
        pb = self.state["peers"].get(b, {"rank": 10 ** 9})
        return a if pa["rank"] <= pb["rank"] else b

    def _escalate(self, th):
        """协商回合/时限耗尽 → 转待裁决，通知权限高的一方定案。"""
        th["status"] = "needs_decision"
        senior = self._senior_party(th)
        self.state["msg_seq"] += 1
        self.state["inbox"].setdefault(senior, []).append({
            "id": self.state["msg_seq"], "from": "bus", "from_name": "总线",
            "body": (f"⚖️ 私聊 {th['id']}「{th['topic']}」协商回合/时限已尽，"
                     f"请你阅读全过程后用 `bus decide {th['id']} \"最终方案\"` 总结定案。"),
            "thread": th["id"], "blocking": True, "ts": now(), "read": False,
        })
        self._emit("thread.escalated", thread=th["id"], topic=th["topic"])

    def sweep(self):
        """惰性巡检：超时私聊转裁决、到期锁释放、死 waiter 清理、交接过期。每次请求时调用。"""
        with self.mu:
            changed = False
            for th in self.state["threads"].values():
                if th.get("blocking") and th["status"] == "open" \
                        and th.get("deadline") and now() > th["deadline"]:
                    self._escalate(th)
                    changed = True
            expired = [k for k, l in self.state["locks"].items()
                       if l.get("expires") and now() > l["expires"]]
            for k in expired:
                l = self.state["locks"].pop(k)
                self._emit("lock.expired", path=l["path"], region=l["region"],
                           owner=l["owner_name"])
                changed = True
            if expired:
                self._grant_waiters()
            before = len(self.state["waiters"])
            self.state["waiters"] = [
                w for w in self.state["waiters"]
                if w["peer"] in self.state["peers"]
                and self.alive(self.state["peers"][w["peer"]])]
            if len(self.state["waiters"]) != before:
                changed = True
            for h in self.state["handoffs"].values():
                if h["status"] == "offered" and h.get("expires") and now() > h["expires"]:
                    h["status"] = "expired"
                    c = self.state["claims"].get(h["claim"])
                    if c and c["status"] == "handing_off":
                        c["status"] = "working"
                        c["updated"] = now()
                    self._clear_pending(h["id"])
                    self._emit("handoff.expired", id=h["id"], claim=h["claim"])
                    changed = True
            if changed:
                self.save()

    def say(self, pid, to, body, blocking, rounds, deadline_min, topic):
        me = self.get_peer(pid)
        with self.mu:
            if to == "all":
                if blocking:
                    raise BusError(400, "公聊不支持阻塞消息，阻塞请用私聊 --blocking")
                self.state["msg_seq"] += 1
                self.state["public"].append({
                    "id": self.state["msg_seq"], "from": pid,
                    "from_name": me["name"], "body": body, "ts": now(),
                })
                self._emit("msg.public", **{"from": me["name"], "body": body[:80]})
                self.save()
                return {"kind": "public"}
            target = self.peer_by_name(to)
            if not target:
                raise BusError(404, f"未知对象：{to}（用 bus peers 查看在线端）")
            if target["id"] == pid:
                raise BusError(400, "不能跟自己私聊")
            self.state["thread_seq"] += 1
            tid = f"t{self.state['thread_seq']}"
            th = {
                "id": tid, "topic": topic or body[:30],
                "parties": [pid, target["id"]], "blocking": bool(blocking),
                "rounds_left": (rounds or DEFAULT_ROUNDS) if blocking else None,
                "deadline": now() + (deadline_min or DEFAULT_DEADLINE_MIN) * 60 if blocking else None,
                "status": "open" if blocking else "chat",
                "messages": [], "resolution": None, "created": now(),
            }
            self.state["threads"][tid] = th
            self._append_msg(th, me, body, notify_pid=target["id"])
            self._emit("thread.created", thread=tid, topic=th["topic"],
                       **{"from": me["name"], "to": target["name"],
                          "blocking": bool(blocking)})
            self.save()
            return {"kind": "dm", "thread": tid}

    def reply(self, pid, tid, body):
        me = self.get_peer(pid)
        with self.mu:
            th = self._thread(tid)
            if pid not in th["parties"]:
                raise BusError(403, "你不是该私聊的参与者")
            if th["status"] in ("resolved", "decided"):
                raise BusError(400, f"该私聊已结束（{th['status']}），有异议请发新私聊")
            self._append_msg(th, me, body, notify_pid=self._other_party(th, pid))
            note = None
            if th["blocking"] and th["status"] == "open":
                th["rounds_left"] -= 1
                if th["rounds_left"] <= 0:
                    self._escalate(th)
                    note = "回合已尽，转入裁决：等待高权限方 decide"
            self._emit("thread.replied", thread=tid, actor=me["name"],
                       rounds_left=th["rounds_left"])
            self.save()
            return {"thread": tid, "rounds_left": th["rounds_left"],
                    "status": th["status"], "note": note}

    def _finish_thread(self, th, pid, summary, verdict):
        me = self.state["peers"][pid]
        th["status"] = verdict
        th["resolution"] = {"by": pid, "by_name": me["name"],
                            "summary": summary, "ts": now()}
        other = self._other_party(th, pid)
        icon = "✅" if verdict == "resolved" else "⚖️"
        label = "共识" if verdict == "resolved" else "裁决"
        # 线程已定案：未读通知降级为非阻塞，不再要求必须处理
        for m in self.state["inbox"].get(other, []):
            if m["thread"] == th["id"] and not m["read"]:
                m["blocking"] = False
        self.state["msg_seq"] += 1
        self.state["inbox"].setdefault(other, []).append({
            "id": self.state["msg_seq"], "from": pid, "from_name": me["name"],
            "body": f"{icon} {me['name']} 对「{th['topic']}」定下{label}：{summary}",
            "thread": th["id"], "blocking": False, "ts": now(), "read": False,
        })
        names = " ↔ ".join(self.state["peers"].get(p, {}).get("name", p)
                           for p in th["parties"])
        self._board_append("决策记录",
                           f"- {icon} **{th['topic']}**（{names}，{label}，{fmt_time()}）：{summary}")
        self._emit("thread." + verdict, thread=th["id"], topic=th["topic"],
                   by=me["name"], summary=summary)

    def resolve(self, pid, tid, summary):
        self.get_peer(pid)
        with self.mu:
            th = self._thread(tid)
            if pid not in th["parties"]:
                raise BusError(403, "你不是该私聊的参与者")
            if th["status"] in ("resolved", "decided"):
                raise BusError(400, "该私聊已结束")
            self._finish_thread(th, pid, summary, "resolved")
            self.save()

    def decide(self, pid, tid, summary):
        me = self.get_peer(pid)
        with self.mu:
            th = self._thread(tid)
            if pid not in th["parties"]:
                raise BusError(403, "你不是该私聊的参与者")
            if th["status"] in ("resolved", "decided"):
                raise BusError(400, "该私聊已结束")
            senior = self._senior_party(th)
            host = self.host_peer()
            is_host = host is not None and host["id"] == pid
            if pid != senior and not is_host:
                sname = self.state["peers"].get(senior, {}).get("name", senior)
                raise BusError(403, f"只有权限高的一方（{sname}）或当前主机可以裁决")
            self._finish_thread(th, pid, summary, "decided")
            self.save()

    # ---- Claim：工作声明（运行时归属，不做任务管理）----

    def claim(self, pid, name, note, scope, status, waiting_on):
        me = self.get_peer(pid)
        if status and status not in self.CLAIM_STATUSES:
            raise BusError(400, f"status 须为 {'/'.join(self.CLAIM_STATUSES)}")
        with self.mu:
            c = self.state["claims"].get(name)
            if c:
                if c["owner"] != pid:
                    raise BusError(409, f"「{name}」已被 {c['owner_name']} 声明"
                                        f"（{c['status']}）。协调用私聊，掉线接管用 bus takeover")
                if note is not None:
                    c["note"] = note
                if scope is not None:
                    c["scope"] = scope
                if status:
                    c["status"] = status
                if waiting_on is not None:
                    c["waiting_on"] = waiting_on
                c["updated"] = now()
                self._emit("claim.updated", claim=name, actor=me["name"],
                           status=c["status"])
            else:
                c = {"name": name, "owner": pid, "owner_name": me["name"],
                     "owner_rank": me["rank"], "note": note or "",
                     "scope": scope or "", "status": status or "claimed",
                     "waiting_on": waiting_on or "",
                     "created": now(), "updated": now()}
                self.state["claims"][name] = c
                self._emit("claim.created", claim=name, actor=me["name"],
                           note=note or "", scope=scope or "")
            self.save()
            return c

    def unclaim(self, pid, name, status):
        me = self.get_peer(pid)
        if status not in ("done", "abandoned"):
            raise BusError(400, "status 须为 done 或 abandoned")
        with self.mu:
            c = self.state["claims"].get(name)
            if not c:
                raise BusError(404, f"claim 不存在：{name}")
            host = self.host_peer()
            if c["owner"] != pid and not (host and host["id"] == pid):
                raise BusError(403, "只有 owner 或当前主机可以关闭 claim")
            c["status"] = status
            c["updated"] = now()
            self._emit("claim.closed", claim=name, actor=me["name"], status=status)
            self.save()

    # ---- Handoff：两阶段交接 + 死后接管 ----

    def _get_handoff(self, hid):
        h = self.state["handoffs"].get(hid)
        if not h:
            raise BusError(404, f"未知交接：{hid}")
        return h

    def _notify(self, pid_to, from_id, from_name, body, blocking=False, thread=None):
        self.state["msg_seq"] += 1
        self.state["inbox"].setdefault(pid_to, []).append({
            "id": self.state["msg_seq"], "from": from_id, "from_name": from_name,
            "body": body, "thread": thread, "blocking": blocking,
            "ts": now(), "read": False,
        })

    def handoff(self, pid, to, claim_name, goal, state_text, blockers,
                next_action, git, wip_patch):
        me = self.get_peer(pid)
        with self.mu:
            c = self.state["claims"].get(claim_name)
            if not c:
                raise BusError(404, f"claim 不存在：{claim_name}（先 bus claim {claim_name}）")
            if c["owner"] != pid:
                raise BusError(403, f"「{claim_name}」属于 {c['owner_name']}")
            if c["status"] == "handing_off":
                raise BusError(409, "已有进行中的交接，等对方 accept/reject 或先 reject 取消")
            if not next_action:
                raise BusError(400, "必须提供 --next：没有下一步的交接是甩锅")
            target = None
            if to and to != "anyone":
                target = self.peer_by_name(to)
                if not target:
                    raise BusError(404, f"未知对象：{to}（用 bus peers 查看）")
                if target["id"] == pid:
                    raise BusError(400, "不能交接给自己")
            my_locks = [l for l in self.state["locks"].values() if l["owner"] == pid]
            self.state["handoff_seq"] += 1
            hid = f"h{self.state['handoff_seq']}"
            patch_file = None
            if wip_patch:
                os.makedirs(os.path.join(self.dir, "capsules"), exist_ok=True)
                patch_file = f"capsules/{hid}-wip.patch"
                with open(os.path.join(self.dir, patch_file), "w", encoding="utf-8") as f:
                    f.write(wip_patch)
            capsule = {
                "goal": goal or c["note"],
                "current_state": state_text or "",
                "blockers": blockers or [],
                "next_action": next_action,
                "locks": [lock_label(l) for l in my_locks],
                "threads": [t["id"] for t in self.state["threads"].values()
                            if pid in t["parties"] and t["status"] in ("open", "needs_decision")],
                "changes": [ch["id"] for ch in self.state["changes"] if ch["peer"] == pid][-5:],
                "git": git or {},
                "wip_patch": patch_file,
                "partial": False,
            }
            h = {"id": hid, "claim": claim_name, "from": pid, "from_name": me["name"],
                 "to": target["id"] if target else None,
                 "to_name": target["name"] if target else "任何人",
                 "status": "offered", "created": now(), "expires": now() + HANDOFF_TTL,
                 "capsule": capsule}
            self.state["handoffs"][hid] = h
            c["status"] = "handing_off"
            c["updated"] = now()
            for l in my_locks:
                l["pending"] = hid
            if target:
                self._notify(target["id"], pid, me["name"],
                             f"🤝 {me['name']} 向你交接「{claim_name}」："
                             f"`bus capsule {hid}` 看详情，`bus accept {hid}` 接收 / "
                             f"`bus reject {hid}` 拒绝（30 分钟有效）",
                             blocking=True)
            self._emit("handoff.offered", id=hid, claim=claim_name,
                       **{"from": me["name"], "to": h["to_name"]})
            self.save()
            return h

    def handoff_accept(self, pid, hid):
        me = self.get_peer(pid)
        with self.mu:
            h = self._get_handoff(hid)  # 过期已由 sweep 处理
            if h["status"] != "offered":
                raise BusError(400, f"交接状态为 {h['status']}，不能接收")
            if h["to"] and h["to"] != pid:
                raise BusError(403, f"该交接指定给 {h['to_name']}")
            c = self.state["claims"][h["claim"]]
            c.update(owner=pid, owner_name=me["name"], owner_rank=me["rank"],
                     status="working", updated=now())
            transferred = []
            for l in self.state["locks"].values():
                if l.get("pending") == hid:
                    l["owner"] = pid
                    l["owner_name"] = me["name"]
                    l["owner_rank"] = me["rank"]
                    del l["pending"]
                    l["expires"] = now() + l.get("lease", LOCK_LEASE)
                    transferred.append(lock_label(l))
            h["status"] = "accepted"
            h["accepted_by"] = me["name"]
            h["accepted_at"] = now()
            self._notify(h["from"], pid, me["name"],
                         f"✅ {me['name']} 接受了交接「{h['claim']}」"
                         f"（转移 {len(transferred)} 个锁）")
            self._emit("handoff.accepted", id=hid, claim=h["claim"], by=me["name"],
                       locks=len(transferred))
            self.save()
            patch_text = None
            pf = h["capsule"].get("wip_patch")
            if pf:
                full = os.path.join(self.dir, pf)
                if os.path.exists(full):
                    with open(full, encoding="utf-8") as f:
                        patch_text = f.read()
            return {"handoff": h, "transferred": transferred, "patch_text": patch_text}

    def handoff_reject(self, pid, hid, reason):
        me = self.get_peer(pid)
        with self.mu:
            h = self._get_handoff(hid)
            if h["status"] != "offered":
                raise BusError(400, f"交接状态为 {h['status']}")
            if pid != h["from"] and h["to"] != pid:
                raise BusError(403, "只有发起方（取消）或接收方（拒绝）可以操作")
            h["status"] = "cancelled" if pid == h["from"] else "rejected"
            h["reason"] = reason or ""
            c = self.state["claims"].get(h["claim"])
            if c and c["status"] == "handing_off":
                c["status"] = "working"
                c["updated"] = now()
            self._clear_pending(hid)
            other = h["to"] if pid == h["from"] else h["from"]
            if other:
                self._notify(other, pid, me["name"],
                             f"✋ {me['name']} {'取消' if pid == h['from'] else '拒绝'}"
                             f"了交接「{h['claim']}」" + (f"：{reason}" if reason else ""))
            self._emit("handoff." + h["status"], id=hid, claim=h["claim"],
                       actor=me["name"], reason=reason or "")
            self.save()

    def takeover(self, pid, claim_name, reason):
        """被动接管：owner 掉线（或权限更高）时，从事故现场合成 salvage capsule。"""
        me = self.get_peer(pid)
        with self.mu:
            c = self.state["claims"].get(claim_name)
            if not c:
                raise BusError(404, f"claim 不存在：{claim_name}")
            if c["owner"] == pid:
                raise BusError(400, f"「{claim_name}」已经是你的")
            owner = self.state["peers"].get(c["owner"])
            owner_alive = bool(owner) and self.alive(owner)
            if owner_alive:
                if me["rank"] > c["owner_rank"]:
                    raise BusError(403, f"{c['owner_name']} 仍在线且权限不低于你。"
                                        "先私聊协商，或请对方 bus handoff")
            else:
                host = self.host_peer()
                dead_for = now() - (owner["last_seen"] if owner else 1e18)
                if not (me["rank"] < c["owner_rank"]
                        or (host and host["id"] == pid)
                        or dead_for > 2 * TTL):
                    raise BusError(403, "对方掉线时间尚短：需更高权限者或当前主机接管；"
                                        "掉线超 20 分钟后任何人可接管")
            # 取消该 claim 上未决的交接
            for h in self.state["handoffs"].values():
                if h["status"] == "offered" and h["claim"] == claim_name:
                    h["status"] = "cancelled"
                    self._clear_pending(h["id"])
            owner_locks = [l for l in self.state["locks"].values()
                           if l["owner"] == c["owner"]]
            if owner_alive:
                salvage_note = "该接管为高权限强制接管，原负责人仍在线、未参与交接"
                salvage_next = "先与原负责人核对现场，避免覆盖其未提交改动"
            else:
                salvage_note = "原负责人掉线，其未提交改动与脑中上下文可能已丢失"
                salvage_next = "先 git log / git status 核对代码现场，再继续"
            salvage = {
                "goal": c["note"],
                "current_state": "（事故现场重建，非本人主动交接）",
                "blockers": [salvage_note],
                "next_action": salvage_next,
                "locks": [lock_label(l) for l in owner_locks],
                "threads": [t["id"] for t in self.state["threads"].values()
                            if c["owner"] in t["parties"]
                            and t["status"] in ("open", "needs_decision")],
                "changes": [ch["id"] for ch in self.state["changes"]
                            if ch["peer"] == c["owner"]][-5:],
                "git": {}, "wip_patch": None, "partial": True,
            }
            self.state["handoff_seq"] += 1
            hid = f"h{self.state['handoff_seq']}"
            h = {"id": hid, "claim": claim_name, "from": c["owner"],
                 "from_name": c["owner_name"], "to": pid, "to_name": me["name"],
                 "status": "taken_over", "created": now(), "expires": None,
                 "capsule": salvage, "reason": reason or ""}
            self.state["handoffs"][hid] = h
            transferred = []
            for l in owner_locks:
                l["owner"] = pid
                l["owner_name"] = me["name"]
                l["owner_rank"] = me["rank"]
                l.pop("pending", None)
                l["expires"] = now() + l.get("lease", LOCK_LEASE)
                transferred.append(lock_label(l))
            c.update(owner=pid, owner_name=me["name"], owner_rank=me["rank"],
                     status="working", updated=now())
            self._emit("work.taken_over", id=hid, claim=claim_name,
                       **{"from": h["from_name"], "to": me["name"],
                          "reason": reason or ""})
            self.save()
            return {"handoff": h, "transferred": transferred,
                    "owner_alive": owner_alive}

    # ---- 改动历史 ----

    def done(self, pid, summary, files, commit, detail):
        me = self.get_peer(pid)
        with self.mu:
            self.state["change_seq"] += 1
            cid = self.state["change_seq"]
            detail_file = None
            if detail:
                os.makedirs(os.path.join(self.dir, "changes"), exist_ok=True)
                slug = re.sub(r"[^\w一-鿿-]+", "-", summary).strip("-")[:40]
                detail_file = f"changes/{cid:04d}-{slug}.md"
                with open(os.path.join(self.dir, detail_file), "w", encoding="utf-8") as f:
                    f.write(f"# {summary}\n\n"
                            f"- 作者: {me['name']} (rank {me['rank']})\n"
                            f"- 时间: {fmt_time()}\n"
                            f"- 文件: {', '.join(files or []) or '-'}\n"
                            f"- commit: {commit or '-'}\n\n{detail}\n")
            self.state["changes"].append({
                "id": cid, "peer": pid, "name": me["name"], "summary": summary,
                "files": files or [], "commit": commit,
                "detail_file": detail_file, "ts": now(),
            })
            self._emit("change.recorded", id=cid, actor=me["name"], summary=summary,
                       files=files or [], commit=commit)
            self.save()
            return cid

    # ---- 黑板 ----

    def _ensure_board(self):
        if not os.path.exists(self.board_path):
            os.makedirs(os.path.dirname(self.board_path), exist_ok=True)
            with open(self.board_path, "w", encoding="utf-8") as f:
                f.write("# 共享黑板\n\n> 所有端共同维护。结论性内容写这里，过程讨论去公聊/私聊。\n")

    def _board_append(self, section, text):
        self._ensure_board()
        with open(self.board_path, encoding="utf-8") as f:
            lines = f.read().splitlines()
        heading = f"## {section}"
        if heading in lines:
            i = lines.index(heading)
            j = i + 1
            while j < len(lines) and not lines[j].startswith("## "):
                j += 1
            while j > i + 1 and lines[j - 1] == "":
                j -= 1
            lines[j:j] = [text, ""]
        else:
            lines += ["", heading, "", text]
        with open(self.board_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    def board_append(self, pid, section, text):
        me = self.get_peer(pid)
        with self.mu:
            self._board_append(section, text)
            self._emit("board.appended", actor=me["name"], section=section)
            self.save()

    def board_reset(self, pid):
        """主机专用：黑板重置为初始模板。历史内容请先 bus archive 归档。"""
        me = self.get_peer(pid)
        with self.mu:
            host = self.host_peer()
            if host is None or host["id"] != pid:
                raise BusError(403, "只有当前主机可以重置黑板（可先 bus takeover 或与主机协调）")
            self._ensure_board()
            with open(self.board_path, "w", encoding="utf-8") as f:
                f.write("# 共享黑板\n\n> 所有端共同维护。结论性内容写这里，过程讨论去公聊/私聊。\n")
            self._emit("board.resetted", actor=me["name"])
            self.save()

    def peer_rm(self, pid, target):
        """主机专用：移除掉线 peer（释放其锁、废弃其 claim、清等待队列）。"""
        me = self.get_peer(pid)
        with self.mu:
            host = self.host_peer()
            if host is None or host["id"] != pid:
                raise BusError(403, "只有当前主机可以移除 peer")
            tgt = next((p for p in self.state["peers"].values()
                        if p["name"] == target or p["id"] == target), None)
            if not tgt:
                raise BusError(404, f"未知 peer: {target}")
            if tgt["id"] == pid:
                raise BusError(400, "不能移除自己")
            released = [k for k, l in self.state["locks"].items()
                        if l["owner"] == tgt["id"]]
            for k in released:
                del self.state["locks"][k]
            self.state["waiters"] = [w for w in self.state["waiters"]
                                     if w["peer"] != tgt["id"]]
            for c in self.state["claims"].values():
                if c["owner"] == tgt["id"] and c["status"] not in ("done", "abandoned"):
                    c["status"] = "abandoned"
                    c["updated"] = now()
            del self.state["peers"][tgt["id"]]
            self._emit("peer.left", peer=tgt["name"], released_locks=len(released),
                       reason="removed-by-host")
            if released:
                self._grant_waiters()
            self.save()
            return {"ok": True, "removed": tgt["name"], "released_locks": len(released)}

    def heartbeat(self, pid):
        """轻量心跳：续期/保活 + 返回待办详情（含未读消息/交接条目），不标记已读。"""
        self.sweep()
        self.get_peer(pid)
        with self.mu:
            inbox = self.state["inbox"].get(pid, [])
            unread = [m for m in inbox if not m["read"]]
            ub = sum(1 for m in unread if m["blocking"])
            un = sum(1 for m in unread if not m["blocking"])
            offers = [h for h in self.state["handoffs"].values()
                      if h["status"] == "offered" and h["to"] in (None, pid)]
            self.save()
            return {
                "unread_blocking": ub,
                "unread_normal": un,
                "handoff_offers": len(offers),
                "blocking_items": [
                    {"thread": m.get("thread"), "from": m.get("from_name"),
                     "body": (m.get("body") or "")[:120]}
                    for m in unread if m["blocking"]],
                "normal_items": [
                    {"thread": m.get("thread"), "from": m.get("from_name"),
                     "body": (m.get("body") or "")[:120]}
                    for m in unread if not m["blocking"]],
                "handoff_items": [
                    {"hid": h["id"], "claim": h["claim"], "from": h["from_name"]}
                    for h in offers],
            }

    def board_read(self):
        with open(self.board_path, encoding="utf-8") as f:
            return f.read()

    # ---- 视图 ----

    def status_view(self):
        self.sweep()
        with self.mu:
            peers = [dict(p, alive=self.alive(p))
                     for p in sorted(self.state["peers"].values(),
                                     key=lambda x: x["rank"])]
            host = self.host_peer()
            return {
                "peers": peers,
                "host": host["id"] if host else None,
                "locks": list(self.state["locks"].values()),
                "waiters": self.state["waiters"],
                "claims": list(self.state["claims"].values()),
                "handoffs": [{k: h.get(k) for k in
                              ("id", "claim", "from_name", "to_name", "status", "created")}
                             for h in self.state["handoffs"].values()],
                "changes": self.state["changes"][-50:],
                "public": self.state["public"][-50:],
                "threads": [{k: t[k] for k in
                             ("id", "topic", "parties", "blocking",
                              "rounds_left", "status")}
                            for t in self.state["threads"].values()],
            }

    def thread_view(self, tid):
        return self._thread(tid)

    def handoff_view(self, hid):
        return self._get_handoff(hid)

    def events_view(self, n, etype):
        if not os.path.exists(self.events_path):
            return []
        with open(self.events_path, encoding="utf-8") as f:
            lines = f.readlines()
        out = []
        for line in lines[-2000:]:
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if etype and not ev.get("type", "").startswith(etype):
                continue
            out.append(ev)
        return out[-n:]

    def sync_view(self, pid):
        """收尾/开场仪式：心跳 + 新公聊 + 新改动 + 未读私聊（阻塞优先）+ 交接 + claims。"""
        self.sweep()
        me = self.get_peer(pid)
        with self.mu:
            cur = me.setdefault("cursors", {"public": 0, "changes": 0})
            new_public = [m for m in self.state["public"] if m["id"] > cur["public"]]
            new_changes = [c for c in self.state["changes"] if c["id"] > cur["changes"]]
            cur["public"] = self.state["msg_seq"]
            cur["changes"] = self.state["change_seq"]

            inbox = self.state["inbox"].get(pid, [])
            unread = [m for m in inbox if not m["read"]]
            for m in unread:
                m["read"] = True

            def enrich(t):
                d = {k: t[k] for k in ("id", "topic", "blocking", "rounds_left", "status")}
                d["parties"] = [self.state["peers"].get(p, {}).get("name", p)
                                for p in t["parties"]]
                d["needs_me"] = (t["status"] == "needs_decision"
                                 and pid == self._senior_party(t))
                return d

            my_threads = [enrich(t) for t in self.state["threads"].values()
                          if pid in t["parties"] and t["status"] in ("open", "needs_decision")]
            offers = [{"id": h["id"], "claim": h["claim"], "from_name": h["from_name"],
                       "capsule": h["capsule"]}
                      for h in self.state["handoffs"].values()
                      if h["status"] == "offered" and h["to"] in (None, pid)]
            active_claims = [c for c in self.state["claims"].values()
                             if c["status"] not in ("done", "abandoned")]
            peers = [{"name": p["name"], "rank": p["rank"], "alive": self.alive(p),
                      "host": p["host"], "cli": p["cli"]}
                     for p in sorted(self.state["peers"].values(), key=lambda x: x["rank"])]
            host = self.host_peer()
            self.save()
            return {
                "me": {"id": me["id"], "name": me["name"], "rank": me["rank"]},
                "host": (host or {}).get("name"),
                "peers": peers,
                "locks": list(self.state["locks"].values()),
                "new_public": new_public,
                "new_changes": new_changes,
                "unread_blocking": [m for m in unread if m["blocking"]],
                "unread_normal": [m for m in unread if not m["blocking"]],
                "my_open_threads": my_threads,
                "handoff_offers": offers,
                "active_claims": active_claims,
            }


# ==================== HTTP 层 ====================

def make_handler(bus):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            if not n:
                return {}
            return json.loads(self.rfile.read(n).decode("utf-8"))

        def _send(self, code, obj):
            data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self._route("GET")

        def do_POST(self):
            self._route("POST")

        def _route(self, method):
            if self.headers.get("X-Bus-Token") != bus.token:
                return self._send(401, {"error": "token 无效"})
            try:
                bus.sweep()
                u = urlparse(self.path)
                path, q = u.path, parse_qs(u.query)
                b = self._body() if method == "POST" else {}

                if method == "POST" and path == "/api/join":
                    return self._send(200, {"peer": bus.join(
                        b.get("name"), b.get("host"), b.get("cli"))})
                if method == "POST" and path == "/api/leave":
                    n = bus.leave(b["peer"])
                    return self._send(200, {"ok": True, "released_locks": n})
                if method == "POST" and path == "/api/lock":
                    return self._send(200, bus.lock(
                        b["peer"], b["path"], b.get("region"), b.get("note"),
                        b.get("ttl_min"), b.get("wait")))
                if method == "POST" and path == "/api/unlock":
                    if b.get("all"):
                        n, skipped = bus.unlock_all(b["peer"])
                        return self._send(200, {"ok": True, "released": n,
                                                "skipped_pending": skipped})
                    bus.unlock(b["peer"], b["path"], b.get("region"), b.get("force"))
                    return self._send(200, {"ok": True})
                if method == "POST" and path == "/api/claim":
                    return self._send(200, {"claim": bus.claim(
                        b["peer"], b["name"], b.get("note"), b.get("scope"),
                        b.get("status"), b.get("waiting_on"))})
                if method == "POST" and path == "/api/unclaim":
                    bus.unclaim(b["peer"], b["name"], b.get("status") or "done")
                    return self._send(200, {"ok": True})
                if method == "POST" and path == "/api/handoff":
                    return self._send(200, {"handoff": bus.handoff(
                        b["peer"], b.get("to"), b["claim"], b.get("goal"),
                        b.get("state"), b.get("blockers"), b.get("next"),
                        b.get("git"), b.get("wip_patch"))})
                if method == "POST" and path == "/api/accept":
                    return self._send(200, bus.handoff_accept(b["peer"], b["id"]))
                if method == "POST" and path == "/api/reject":
                    bus.handoff_reject(b["peer"], b["id"], b.get("reason"))
                    return self._send(200, {"ok": True})
                if method == "POST" and path == "/api/takeover":
                    return self._send(200, bus.takeover(
                        b["peer"], b["claim"], b.get("reason")))
                if method == "POST" and path == "/api/say":
                    return self._send(200, bus.say(
                        b["peer"], b["to"], b["body"], b.get("blocking"),
                        b.get("rounds"), b.get("deadline_min"), b.get("topic")))
                if method == "POST" and path == "/api/reply":
                    return self._send(200, bus.reply(b["peer"], b["thread"], b["body"]))
                if method == "POST" and path == "/api/resolve":
                    bus.resolve(b["peer"], b["thread"], b["summary"])
                    return self._send(200, {"ok": True})
                if method == "POST" and path == "/api/decide":
                    bus.decide(b["peer"], b["thread"], b["summary"])
                    return self._send(200, {"ok": True})
                if method == "POST" and path == "/api/done":
                    cid = bus.done(b["peer"], b["summary"], b.get("files"),
                                   b.get("commit"), b.get("detail"))
                    return self._send(200, {"id": cid})
                if method == "POST" and path == "/api/board":
                    bus.board_append(b["peer"], b["section"], b["text"])
                    return self._send(200, {"ok": True})
                if method == "POST" and path == "/api/board/reset":
                    bus.board_reset(b["peer"])
                    return self._send(200, {"ok": True})
                if method == "POST" and path == "/api/peers/rm":
                    return self._send(200, bus.peer_rm(b["peer"], b["name"]))
                if method == "POST" and path == "/api/heartbeat":
                    return self._send(200, bus.heartbeat(b["peer"]))
                if method == "GET" and path == "/api/sync":
                    return self._send(200, bus.sync_view(q["peer"][0]))
                if method == "GET" and path == "/api/status":
                    return self._send(200, bus.status_view())
                if method == "GET" and path == "/api/thread":
                    return self._send(200, bus.thread_view(q["id"][0]))
                if method == "GET" and path == "/api/handoff":
                    return self._send(200, bus.handoff_view(q["id"][0]))
                if method == "GET" and path == "/api/events":
                    return self._send(200, {"events": bus.events_view(
                        int(q.get("n", ["30"])[0]), q.get("type", [None])[0])})
                if method == "GET" and path == "/api/board":
                    return self._send(200, {"board": bus.board_read()})
                return self._send(404, {"error": f"未知路由: {method} {path}"})
            except BusError as e:
                return self._send(e.code, {"error": e.msg})
            except (KeyError, ValueError, TypeError) as e:
                return self._send(400, {"error": f"参数错误: {e}"})
            except OSError as e:
                return self._send(500, {"error": f"hub 存储错误: {e}"})
            except Exception as e:
                return self._send(500, {"error": f"hub 内部错误: {type(e).__name__}: {e}"})

    return Handler


def guess_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return "127.0.0.1"


def tailscale_ip():
    """本机 Tailscale IPv4（100.64.0.0/10）；未安装/未运行返回 None。"""
    import shutil
    ts = shutil.which("tailscale")
    if not ts:
        return None
    try:
        r = subprocess.run([ts, "ip", "-4"], capture_output=True, text=True, timeout=5)
        ip = (r.stdout.strip().splitlines() or [""])[0]
        return ip if r.returncode == 0 and ip.startswith("100.") else None
    except Exception:
        return None


def cmd_serve(args):
    bus = Bus(args.dir)
    lan_ip = guess_ip()
    ts_ip = tailscale_ip()
    # 广告地址优先级：--host 显式指定 > Tailscale（跨网络可达且加密）> 局域网
    primary = args.host or ts_ip or lan_ip
    urls = []
    for ip in (primary, ts_ip, lan_ip, "127.0.0.1"):
        if ip:
            u = f"http://{ip}:{args.port}"
            if u not in urls:
                urls.append(u)
    with open(os.path.join(bus.dir, "hub.json"), "w", encoding="utf-8") as f:
        json.dump({"url": urls[0], "alt_urls": urls[1:], "token": bus.token}, f)
    print(f"agent-bus hub v{VERSION} 已启动（数据目录: {bus.dir}）")
    for i, u in enumerate(urls):
        tag = ""
        if ts_ip and ts_ip in u:
            tag = "（Tailscale，推荐跨网络使用）"
        elif lan_ip and lan_ip in u:
            tag = "（局域网）"
        elif "127.0.0.1" in u:
            tag = "（仅本机）"
        mark = "★" if i == 0 else " "
        print(f" {mark} bus join --hub '{u}#{bus.token}'  {tag}")
    if not ts_ip:
        print("  提示：未检测到 Tailscale。多机跨网络协作建议安装（https://tailscale.com），"
              "装好后重启 serve 即自动广告 Tailscale 地址，零额外配置。")
    apath = os.path.realpath(bus.dir)
    tmp_roots = [os.path.realpath(r) for r in
                 ("/tmp", "/private/tmp", "/var/tmp", tempfile.gettempdir())]
    if apath in tmp_roots or any(apath.startswith(r + os.sep) for r in tmp_roots):
        print("  ⚠ 数据目录在系统临时目录下，可能被系统定期清理"
              "（如 macOS periodic daily 按 atime 清理超 3 天未访问文件），"
              "导致 board.md/hub.json 等静默丢失、board 写入崩溃。"
              "建议迁到项目目录：停 serve → 移动数据目录 → 原目录重新 serve"
              "（token 不变，对端无需重新 join）。")
    print(f"（若 {args.dir} 随项目 git 同步，对端也可直接 bus join 自动发现+自动选路）")
    httpd = ThreadingHTTPServer(("", args.port), make_handler(bus))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


# ==================== 客户端 CLI ====================

def die(msg):
    print(f"✗ {msg}", file=sys.stderr)
    sys.exit(1)


def peer_file(name=None):
    """同项目多会话并存：--name 指定后身份文件分开存，也可用 BUS_PEER_FILE 覆盖。"""
    if os.environ.get("BUS_PEER_FILE"):
        return os.environ["BUS_PEER_FILE"]
    return f".bus-peer.{name}.json" if name else PEER_FILE


def load_conf():
    if os.environ.get("BUS_PEER_FILE"):
        path = os.environ["BUS_PEER_FILE"]
        if not os.path.exists(path):
            die(f"身份文件不存在: {path}")
    elif os.path.exists(PEER_FILE):
        path = PEER_FILE
    else:
        import glob
        cands = sorted(glob.glob(".bus-peer.*.json"))
        if len(cands) == 1:
            path = cands[0]
        elif not cands:
            die("未 join。先运行: bus join --hub <url#token> [--name 你的名字]")
        else:
            die(f"本目录有多个身份（{', '.join(cands)}），"
                "请用环境变量 BUS_PEER_FILE 指定当前会话用哪个")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def api_soft(conf, method, path, payload=None, timeout=15):
    """返回 (ok, result)；失败不退出，供 hook 等需要 fail-open 的场景。"""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        conf["hub"] + path, data=data, method=method,
        headers={"X-Bus-Token": conf["token"],
                 "Content-Type": "application/json"})
    # hub 通常是本机/局域网地址，绕过系统代理（macOS 上 urllib 会读系统代理设置）
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as r:
            return True, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode("utf-8")).get("error")
        except Exception:
            msg = str(e)
        return False, {"error": msg}
    except Exception as e:
        return False, {"error": f"连不上 hub（{conf['hub']}）：{e}"}


def api(conf, method, path, payload=None):
    ok, r = api_soft(conf, method, path, payload)
    if not ok:
        die(r["error"])
    return r


def parse_hub(s):
    """http://host:port#token → (url, token)"""
    if "#" in s:
        url, token = s.rsplit("#", 1)
        return url.rstrip("/"), token
    die("--hub 格式应为 http://host:port#token")


def git_run(*a):
    try:
        r = subprocess.run(["git", *a], capture_output=True, text=True, timeout=10)
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def git_head():
    return git_run("rev-parse", "--short", "HEAD")


def git_info():
    head = git_head()
    if not head:
        return {}
    return {"branch": git_run("rev-parse", "--abbrev-ref", "HEAD"),
            "head": head,
            "dirty": bool(git_run("status", "--porcelain"))}


def probe_hub(url, timeout=2):
    """探测 hub 可达性：收到任何 HTTP 响应（含 401）即视为可达。"""
    req = urllib.request.Request(url.rstrip("/") + "/api/status", method="GET")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout):
            return True
    except urllib.error.HTTPError:
        return True  # 401 = 服务在线
    except Exception:
        return False


def cmd_join(args):
    if args.hub:
        url, token = parse_hub(args.hub)
    else:
        hj = os.path.join(".bus", "hub.json")
        if not os.path.exists(hj):
            die("缺少 --hub，且本地 .bus/hub.json 不存在（先 git pull 或向主机要 hub 地址）")
        with open(hj, encoding="utf-8") as f:
            c = json.load(f)
        token = c["token"]
        # 自动选路：逐个探测候选地址（含 Tailscale / 局域网备选）
        candidates = [c["url"]] + c.get("alt_urls", [])
        url = None
        for cand in candidates:
            if probe_hub(cand):
                url = cand
                break
        if not url:
            die("hub.json 里的地址都不可达：\n  " + "\n  ".join(candidates)
                + "\n检查网络/Tailscale，或用 --hub 手动指定")
        if url != c["url"]:
            print(f"  （自动选路：主地址不可达，改用 {url}）")
    host = socket.gethostname()
    name = args.name or f"{host}-{os.path.basename(os.getcwd())}"
    r = api({"hub": url, "token": token}, "POST", "/api/join",
            {"name": name, "host": host, "cli": args.cli or os.environ.get("BUS_CLI", "?")})
    p = r["peer"]
    path = peer_file(args.name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"hub": url, "token": token, "peer_id": p["id"],
                   "name": p["name"], "rank": p["rank"]}, f, ensure_ascii=False, indent=1)
    role = "主机（rank 0，权限最高）" if p["rank"] == 0 else f"rank {p['rank']}"
    print(f"✓ 已加入总线：{p['name']}，{role}，身份写入 {path}")
    print("  下一步：bus sync 查看公聊、改动、私聊与交接")


def cmd_sync(args):
    conf = load_conf()
    v = api(conf, "GET", f"/api/sync?peer={conf['peer_id']}")
    me = v["me"]
    role = " 👑主机" if v["host"] == me["name"] else ""
    online = [p for p in v["peers"] if p["alive"]]
    print(f"== sync == {me['name']} (rank {me['rank']}{role}) | 在线 {len(online)} 端: "
          + ", ".join(f"{p['name']}(r{p['rank']})" for p in online))

    if v["new_changes"]:
        print(f"\n-- 新改动 ({len(v['new_changes'])}) --")
        for c in v["new_changes"]:
            line = f"  #{c['id']} [{c['name']}] {c['summary']}"
            if c.get("commit"):
                line += f"  (commit {c['commit']})"
            print(line)
            if c.get("files"):
                print(f"      文件: {', '.join(c['files'])}")
            if c.get("detail_file"):
                print(f"      详情: {c['detail_file']}")

    if v["new_public"]:
        print(f"\n-- 新公聊 ({len(v['new_public'])}) --")
        for m in v["new_public"]:
            print(f"  [{fmt_time(m['ts'])}] {m['from_name']}: {m['body']}")

    if v["unread_blocking"]:
        print(f"\n-- ⚠️ 阻塞私聊/交接 ({len(v['unread_blocking'])})，必须处理 --")
        for m in v["unread_blocking"]:
            print(f"  [{m.get('thread') or '-'}] {m['from_name']}: {m['body']}")
        print("  → bus thread <id> 看私聊全文；bus capsule <id> 看交接详情")

    if v["unread_normal"]:
        print(f"\n-- 私聊/通知 ({len(v['unread_normal'])}) --")
        for m in v["unread_normal"]:
            print(f"  [{m.get('thread') or '-'}] {m['from_name']}: {m['body']}")

    if v.get("handoff_offers"):
        print(f"\n-- 🤝 待接收交接 ({len(v['handoff_offers'])}) --")
        for o in v["handoff_offers"]:
            print(f"  [{o['id']}] {o['from_name']} → 你：「{o['claim']}」"
                  f"  目标: {o['capsule'].get('goal') or '-'}")
            print(f"      bus accept {o['id']} 接收 / bus reject {o['id']} 拒绝")

    if v["my_open_threads"]:
        print("\n-- 我参与的进行中私聊 --")
        for t in v["my_open_threads"]:
            tag = "阻塞" if t["blocking"] else "普通"
            extra = ""
            if t["status"] == "needs_decision":
                extra = "，⚖️ 等" + ("你" if t["needs_me"] else "对方高权限方") + "裁决"
            elif t["rounds_left"] is not None:
                extra = f"，剩 {t['rounds_left']} 回合"
            print(f"  [{t['id']}] {t['topic']}（{tag}，{' ↔ '.join(t['parties'])}{extra}）")

    if v.get("active_claims"):
        print(f"\n-- 工作声明 ({len(v['active_claims'])}) --")
        for c in v["active_claims"]:
            w = f"，等待 {c['waiting_on']}" if c.get("waiting_on") else ""
            s = f"，scope {c['scope']}" if c.get("scope") else ""
            mine = "（我）" if c["owner"] == me["id"] else ""
            print(f"  {c['name']} [{c['status']}] ← {c['owner_name']}{mine}{s}{w}")

    if v["locks"]:
        print(f"\n-- 当前锁 ({len(v['locks'])}) --")
        for l in v["locks"]:
            n = f" — {l['note']}" if l.get("note") else ""
            pend = f" [交接 {l['pending']} 中]" if l.get("pending") else ""
            print(f"  {lock_label(l)} ← {l['owner_name']}{n}{pend}")

    if not any([v["new_changes"], v["new_public"], v["unread_blocking"],
                v["unread_normal"], v["my_open_threads"], v.get("handoff_offers")]):
        print("  没有新消息。")


def cmd_status(args):
    conf = load_conf()
    v = api(conf, "GET", "/api/status")
    print("-- peers --")
    for p in v["peers"]:
        mark = "👑" if p["id"] == v["host"] else " "
        state = "在线" if p["alive"] else "掉线"
        print(f" {mark} rank{p['rank']}  {p['name']}  ({p['host']}/{p['cli']})  {state}")
    print(f"-- 工作声明 ({len(v['claims'])}) --")
    for c in v["claims"]:
        w = f"，等待 {c['waiting_on']}" if c.get("waiting_on") else ""
        print(f"  {c['name']} [{c['status']}] ← {c['owner_name']}{w}")
    print(f"-- 锁 ({len(v['locks'])}) --")
    for l in v["locks"]:
        pend = f" [交接 {l['pending']} 中]" if l.get("pending") else ""
        print(f"  {lock_label(l)} ← {l['owner_name']}  {l.get('note', '')}{pend}")
    if v.get("waiters"):
        print(f"-- 锁等待队列 ({len(v['waiters'])}) --")
        for w in v["waiters"]:
            print(f"  {w['peer_name']} 等 {w['path']}"
                  + (f"({w['region']})" if w.get("region") else ""))
    if v.get("handoffs"):
        print(f"-- 交接 ({len(v['handoffs'])}) --")
        for h in v["handoffs"]:
            print(f"  [{h['id']}] {h['claim']}  {h['from_name']} → {h['to_name']}  {h['status']}")
    print(f"-- 私聊线程 ({len(v['threads'])}) --")
    for t in v["threads"]:
        print(f"  [{t['id']}] {t['topic']}  {t['status']}")


def cmd_lock(args):
    conf = load_conf()
    r = api(conf, "POST", "/api/lock",
            {"peer": conf["peer_id"], "path": args.path, "region": args.region,
             "note": args.note, "ttl_min": args.ttl, "wait": args.wait})
    label = norm_path(args.path) + (f"({args.region})" if args.region else "")
    if r.get("waiting"):
        print(f"⏳ {label} 被 {r['holder']} 持有，你已排队（第 {r['position']} 位），"
              "释放后自动获得并通知你")
    else:
        ttl = f"，租约 {args.ttl} 分钟" if args.ttl else ""
        print(f"✓ 已锁 {label}{ttl}")


def cmd_unlock(args):
    conf = load_conf()
    if args.all:
        r = api(conf, "POST", "/api/unlock", {"peer": conf["peer_id"], "all": True})
        msg = f"✓ 已释放 {r['released']} 个锁"
        if r.get("skipped_pending"):
            msg += f"（{r['skipped_pending']} 个在交接中，未释放）"
        print(msg)
        return
    if not args.path:
        die("需要路径，或 --all")
    api(conf, "POST", "/api/unlock", {"peer": conf["peer_id"], "path": args.path,
                                      "region": args.region, "force": args.force})
    print(f"✓ 已解锁 {norm_path(args.path)}")


def cmd_locks(args):
    conf = load_conf()
    v = api(conf, "GET", "/api/status")
    if not v["locks"]:
        print("当前没有锁。")
    for l in v["locks"]:
        n = f" — {l['note']}" if l.get("note") else ""
        pend = f" [交接 {l['pending']} 中]" if l.get("pending") else ""
        print(f"  {lock_label(l)} ← {l['owner_name']}(rank{l['owner_rank']}){n}{pend}")
    if v.get("waiters"):
        print("等待队列：")
        for w in v["waiters"]:
            print(f"  {w['peer_name']} 等 {w['path']}"
                  + (f"({w['region']})" if w.get("region") else ""))


def cmd_claim(args):
    conf = load_conf()
    r = api(conf, "POST", "/api/claim",
            {"peer": conf["peer_id"], "name": args.name, "note": args.note,
             "scope": args.scope, "status": args.status,
             "waiting_on": args.waiting_on})
    c = r["claim"]
    print(f"✓ 「{c['name']}」 owner={c['owner_name']} status={c['status']}"
          + (f" scope={c['scope']}" if c.get("scope") else "")
          + (f" waiting_on={c['waiting_on']}" if c.get("waiting_on") else ""))


def cmd_claims(args):
    conf = load_conf()
    v = api(conf, "GET", "/api/status")
    claims = v["claims"] if args.all else \
        [c for c in v["claims"] if c["status"] not in ("done", "abandoned")]
    if not claims:
        print("没有进行中的工作声明。")
        return
    for c in claims:
        w = f"，等待 {c['waiting_on']}" if c.get("waiting_on") else ""
        s = f"，scope {c['scope']}" if c.get("scope") else ""
        n = f"\n      {c['note']}" if c.get("note") else ""
        print(f"  {c['name']} [{c['status']}] ← {c['owner_name']}{s}{w}{n}")


def cmd_unclaim(args):
    conf = load_conf()
    api(conf, "POST", "/api/unclaim",
        {"peer": conf["peer_id"], "name": args.name,
         "status": "abandoned" if args.abandoned else "done"})
    print(f"✓ 「{args.name}」已关闭（{'abandoned' if args.abandoned else 'done'}）")


def print_capsule(h):
    cap = h["capsule"]
    print(f"[{h['id']}] 「{h['claim']}」 {h['from_name']} → {h['to_name']}  状态 {h['status']}")
    if cap.get("partial"):
        print("  ⚠ 事故现场重建（partial），信息可能不全")
    print(f"  目标: {cap.get('goal') or '-'}")
    if cap.get("current_state"):
        print(f"  现状: {cap['current_state']}")
    for b in cap.get("blockers", []):
        print(f"  ⚠ blocker: {b}")
    print(f"  下一步: {cap.get('next_action') or '-'}")
    if cap.get("locks"):
        print(f"  锁: {', '.join(cap['locks'])}")
    if cap.get("threads"):
        print(f"  相关私聊: {', '.join(cap['threads'])}（bus thread <id> 查看）")
    if cap.get("changes"):
        print(f"  相关改动: {', '.join('#' + str(i) for i in cap['changes'])}（bus log 查看）")
    g = cap.get("git") or {}
    if g:
        print(f"  git: {g.get('branch', '?')} @ {g.get('head', '?')}"
              + ("（有未提交改动）" if g.get("dirty") else ""))


def cmd_handoff(args):
    conf = load_conf()
    git = git_info()
    patch = None
    if git.get("dirty"):
        if args.patch:
            patch = git_run("diff", "HEAD") or ""
            if not patch.strip():
                git["dirty"] = False
                patch = None
        else:
            die("工作区有未提交改动。先 commit，或加 --patch 将 diff 随交接携带")
    r = api(conf, "POST", "/api/handoff",
            {"peer": conf["peer_id"], "to": args.target, "claim": args.claim,
             "goal": args.goal, "state": args.state,
             "blockers": [b.strip() for b in args.blockers.split(",")] if args.blockers else [],
             "next": args.next, "git": git, "wip_patch": patch})
    h = r["handoff"]
    print(f"✓ 交接 {h['id']} 已发出：「{h['claim']}」→ {h['to_name']}（30 分钟有效）")
    print("  你的锁已打上交接标记，对方 accept 时原子转移；reject/超时自动解除")
    if patch:
        print("  已携带 wip patch，对方 accept 后可 git apply")


def cmd_capsule(args):
    conf = load_conf()
    h = api(conf, "GET", f"/api/handoff?id={args.id}")
    print_capsule(h)


def cmd_accept(args):
    conf = load_conf()
    r = api(conf, "POST", "/api/accept", {"peer": conf["peer_id"], "id": args.id})
    h = r["handoff"]
    print(f"✓ 已接管「{h['claim']}」（来自 {h['from_name']}）")
    print_capsule(h)
    if r["transferred"]:
        print(f"  ✓ 已转移锁: {', '.join(r['transferred'])}")
    if r.get("patch_text"):
        fn = f"{h['id']}-wip.patch"
        with open(fn, "w", encoding="utf-8") as f:
            f.write(r["patch_text"])
        print(f"  ⚠ 有未提交改动补丁 → {fn}，审阅后执行 git apply {fn}")


def cmd_reject(args):
    conf = load_conf()
    api(conf, "POST", "/api/reject",
        {"peer": conf["peer_id"], "id": args.id,
         "reason": " ".join(args.reason)})
    print(f"✓ 已拒绝/取消交接 {args.id}，claim 与锁已还原")


def cmd_takeover(args):
    conf = load_conf()
    r = api(conf, "POST", "/api/takeover",
            {"peer": conf["peer_id"], "claim": args.claim, "reason": args.reason})
    h = r["handoff"]
    how = "仍在线（高权限强制接管）" if r.get("owner_alive") else "掉线"
    print(f"✓ 已接管「{h['claim']}」（原 owner {h['from_name']} {how}）")
    print_capsule(h)
    if r["transferred"]:
        print(f"  ✓ 已转移锁: {', '.join(r['transferred'])}")


def cmd_done(args):
    conf = load_conf()
    files = [f.strip() for f in args.files.split(",")] if args.files else []
    commit = args.commit or git_head()
    r = api(conf, "POST", "/api/done",
            {"peer": conf["peer_id"], "summary": args.summary,
             "files": files, "commit": commit, "detail": args.detail})
    print(f"✓ 改动已记录 #{r['id']}" + (f"（commit {commit}）" if commit else ""))
    print("  别忘了：bus unlock --all && bus sync")


def cmd_log(args):
    conf = load_conf()
    v = api(conf, "GET", "/api/status")
    if not v["changes"]:
        print("还没有改动记录。")
        return
    for c in reversed(v["changes"][-args.n:]):
        line = f"#{c['id']} [{fmt_time(c['ts'])}] {c['name']}: {c['summary']}"
        if c.get("commit"):
            line += f"  ({c['commit']})"
        print(line)
        if c.get("files"):
            print(f"    文件: {', '.join(c['files'])}")


def cmd_events(args):
    conf = load_conf()
    path = f"/api/events?n={args.n}" + (f"&type={args.type}" if args.type else "")
    r = api(conf, "GET", path)
    if not r["events"]:
        print("没有匹配的事件。")
        return
    for ev in r["events"]:
        detail = ", ".join(f"{k}={v}" for k, v in ev.items()
                           if k not in ("seq", "ts", "type"))
        print(f"  #{ev['seq']} [{fmt_time(ev['ts'])}] {ev['type']}  {detail}")


def cmd_say(args):
    conf = load_conf()
    body = " ".join(args.message)
    r = api(conf, "POST", "/api/say",
            {"peer": conf["peer_id"], "to": args.target, "body": body,
             "blocking": args.blocking, "rounds": args.rounds,
             "deadline_min": args.deadline, "topic": args.topic})
    if r["kind"] == "public":
        print("✓ 已发到公聊")
    else:
        tag = "阻塞型" if args.blocking else "普通"
        print(f"✓ 已发起{tag}私聊 {r['thread']} → {args.target}")


def cmd_reply(args):
    conf = load_conf()
    r = api(conf, "POST", "/api/reply",
            {"peer": conf["peer_id"], "thread": args.thread,
             "body": " ".join(args.message)})
    extra = f"，剩 {r['rounds_left']} 回合" if r.get("rounds_left") is not None else ""
    print(f"✓ 已回复 {r['thread']}（{r['status']}{extra}）")
    if r.get("note"):
        print(f"  ⚖️ {r['note']}")


def cmd_resolve(args):
    conf = load_conf()
    api(conf, "POST", "/api/resolve",
        {"peer": conf["peer_id"], "thread": args.thread,
         "summary": " ".join(args.summary)})
    print(f"✓ {args.thread} 已达成共识并归档到黑板「决策记录」")


def cmd_decide(args):
    conf = load_conf()
    api(conf, "POST", "/api/decide",
        {"peer": conf["peer_id"], "thread": args.thread,
         "summary": " ".join(args.summary)})
    print(f"⚖️ {args.thread} 已由你裁决定案，归档到黑板「决策记录」")


def cmd_thread(args):
    conf = load_conf()
    t = api(conf, "GET", f"/api/thread?id={args.id}")
    tag = "阻塞" if t["blocking"] else "普通"
    print(f"[{t['id']}] {t['topic']}（{tag}，状态 {t['status']}）")
    for m in t["messages"]:
        print(f"  [{fmt_time(m['ts'])}] {m['from_name']}: {m['body']}")
    if t.get("resolution"):
        r = t["resolution"]
        print(f"  → 结论（{r['by_name']}）: {r['summary']}")


def cmd_chat(args):
    conf = load_conf()
    v = api(conf, "GET", "/api/status")
    if not v["public"]:
        print("公聊还没有消息。")
        return
    for m in v["public"][-args.n:]:
        print(f"  [{fmt_time(m['ts'])}] {m['from_name']}: {m['body']}")


def cmd_board(args):
    conf = load_conf()
    if args.board_cmd == "add":
        api(conf, "POST", "/api/board",
            {"peer": conf["peer_id"], "section": args.section,
             "text": " ".join(args.text)})
        print(f"✓ 已写入黑板「{args.section}」")
    elif args.board_cmd == "reset":
        api(conf, "POST", "/api/board/reset", {"peer": conf["peer_id"]})
        print("✓ 黑板已重置为初始模板（历史内容请先 bus archive 归档）")
    else:
        v = api(conf, "GET", "/api/board")
        print(v["board"])


def cmd_archive(args):
    """把总线内容（黑板/公聊/私聊/改动/声明/事件）归档到本地目录，供重置或审计。"""
    conf = load_conf()
    out = os.path.abspath(args.path)
    os.makedirs(out, exist_ok=True)
    os.makedirs(os.path.join(out, "threads"), exist_ok=True)
    st = api(conf, "GET", "/api/status")
    board = api(conf, "GET", "/api/board")["board"]
    with open(os.path.join(out, "board.md"), "w", encoding="utf-8") as f:
        f.write(board)

    def _write(name, content):
        with open(os.path.join(out, name), "w", encoding="utf-8") as f:
            f.write(content)

    chat = "\n".join(f"[{fmt_time(m['ts'])}] {m['from_name']}: {m['body']}"
                     for m in st["public"]) or "公聊还没有消息。"
    _write("chat.md", chat + "\n")
    changes = "\n".join(
        f"- #{c['id']} [{fmt_time(c['ts'])}] {c['name']}: {c['summary']}"
        f" files={c.get('files')} commit={c.get('commit')}"
        for c in st["changes"]) or "还没有改动记录。"
    _write("changes.md", changes + "\n")
    claims = "\n".join(
        f"- {c['name']} [{c['status']}] owner={c.get('owner_name')}"
        f" scope={c.get('scope')} note={c.get('note')}"
        for c in st["claims"]) or "没有工作声明。"
    _write("claims.md", claims + "\n")
    for t in st["threads"]:
        th = api(conf, "GET", f"/api/thread?id={t['id']}")
        body = f"# {t['id']} [{t['status']}] {t['topic']}\n\n"
        body += f"blocking={t['blocking']} rounds_left={t['rounds_left']}\n\n"
        for m in th.get("messages", []):
            body += f"[{fmt_time(m['ts'])}] {m['from_name']}: {m['body']}\n\n"
        _write(f"threads/{t['id']}.md", body)
    print(f"✓ 已归档到 {out}（board.md / chat.md / changes.md / claims.md / threads/）")


def cmd_peers(args):
    conf = load_conf()
    if args.rm == "rm":
        if not args.name:
            die("用法: bus peers rm <名字>")
        r = api(conf, "POST", "/api/peers/rm",
                {"peer": conf["peer_id"], "name": args.name})
        print(f"✓ 已移除 peer「{r['removed']}」，释放 {r['released_locks']} 个锁")
        return
    v = api(conf, "GET", "/api/status")
    for p in v["peers"]:
        mark = "👑" if p["id"] == v["host"] else " "
        state = "在线" if p["alive"] else "掉线"
        print(f" {mark} rank{p['rank']}  {p['name']}  ({p['host']}/{p['cli']})  {state}")


def cmd_leave(args):
    conf = load_conf()
    r = api(conf, "POST", "/api/leave", {"peer": conf["peer_id"]})
    print(f"✓ 已离开总线，释放了 {r['released_locks']} 个锁，未决交接已取消")


# ==================== 网络：Tailscale 状态与引导安装 ====================

def run_step(desc, cmd, sudo=False):
    """执行一步安装操作；sudo 步骤先明示再跑（密码/登录需人工）。"""
    print(f"→ {desc}")
    print(f"  $ {' '.join(cmd)}")
    r = subprocess.run(cmd)  # 继承终端 stdio，sudo/登录可交互
    if r.returncode != 0:
        print(f"  ✗ 失败（exit {r.returncode}）。")
        return False
    print("  ✓")
    return True


def cmd_net(args):
    import shutil
    sub = getattr(args, "net_cmd", None)
    ts = shutil.which("tailscale")
    ip = tailscale_ip()

    if sub in (None, "status"):
        if ip:
            print(f"✓ Tailscale 已接入，本机地址 {ip}")
            print("  bus serve 会自动广告此地址，对端 join 自动选路，无需任何配置。")
        elif ts:
            print("⚠ Tailscale 已安装但未接入（tailscale up 未完成）。")
            print("  运行 bus net setup 继续引导。")
        else:
            print("✗ 未检测到 Tailscale。")
            print("  运行 bus net setup 自动安装（macOS 用 Homebrew，Linux 用官方脚本）；")
            print("  或手动安装：https://tailscale.com/download")
        return

    if sub == "setup":
        if ip:
            print(f"✓ Tailscale 已就绪（{ip}），无需安装。")
            return
        print("agent-bus 网络引导：将安装并接入 Tailscale。")
        print("过程中会有两处需要人工：sudo 密码、浏览器登录 tailnet 授权。\n")
        if not ts:
            if sys.platform == "darwin":
                brew = shutil.which("brew")
                if not brew:
                    print("✗ 未检测到 Homebrew。请先装 brew（https://brew.sh），"
                          "或从 https://tailscale.com/download/mac 安装 Tailscale GUI 版。")
                    return
                if not run_step("安装 tailscale（Homebrew formula）",
                                [brew, "install", "tailscale"]):
                    print("可改用 GUI 版：https://tailscale.com/download/mac")
                    return
            elif sys.platform.startswith("linux"):
                print("→ 使用 Tailscale 官方安装脚本（自动识别发行版）")
                print("  $ curl -fsSL https://tailscale.com/install.sh | sh")
                r = subprocess.run(
                    "curl -fsSL https://tailscale.com/install.sh | sh", shell=True)
                if r.returncode != 0:
                    print("  ✗ 安装脚本失败，请手动安装：https://tailscale.com/download")
                    return
                print("  ✓")
            else:
                print(f"✗ 暂不支持的平台（{sys.platform}），请手动安装："
                      "https://tailscale.com/download")
                return
            ts = shutil.which("tailscale")
        # 启动守护进程
        if sys.platform == "darwin":
            if not run_step("启动 tailscaled 守护进程（需 sudo 密码）",
                            ["sudo", "brew", "services", "start", "tailscale"], sudo=True):
                return
        elif sys.platform.startswith("linux"):
            if not run_step("启动 tailscaled 守护进程（需 sudo 密码）",
                            ["sudo", "systemctl", "enable", "--now", "tailscaled"], sudo=True):
                return
        # 接入 tailnet（浏览器授权）
        print("→ 接入 tailnet：接下来会弹出浏览器登录地址，完成授权即可")
        up = ["tailscale", "up"]
        r = subprocess.run(up)
        if r.returncode != 0:
            r = subprocess.run(["sudo"] + up)
        ip = tailscale_ip()
        if ip:
            print(f"\n✓ 全部就绪，本机 Tailscale 地址：{ip}")
            print("  重启 bus serve 即自动广告该地址；对端 bus join 自动选路。")
        else:
            print("\n⚠ 安装完成但未检测到 100.x 地址，请检查 `tailscale status` 输出，"
                  "或把输出发来排查。")
        return

    die("用法：bus net [status|setup]")


# ==================== CLI hooks（自动锁 / 自动同步）====================
#
# 架构：业务逻辑只此一份（bus hook <cli>），各 CLI 只差一层薄适配：
#   claude / kimi / codex —— 配置文件注册命令，stdin 收各家 JSON
#   opencode / pi        —— 生成的 TS 胶水层把 payload 归一化后调 bus hook
# 协议：exit 0 放行（stdout 可带提示），exit 2 拦截（stderr 为原因）。
# hub 不可达默认 fail-open（放行），BUS_HOOK_ENFORCE=1 改为 fail-closed。

WRITE_TOOL_RE = re.compile(r"edit|write|notebook|patch|replace|create", re.I)
BASH_TOOL_RE = re.compile(r"bash|shell|exec|terminal|command", re.I)


def hook_normalize(cli, p):
    """把各 CLI 的 payload 归一化成 {event, tool, file_path, command, cwd}。"""
    if cli in ("opencode", "pi"):
        return p  # TS 胶水层已按此格式归一化
    ev = p.get("hook_event_name") or ""
    ti = p.get("tool_input") or {}
    return {
        "event": {"PreToolUse": "pre_tool_use", "Stop": "stop",
                  "SessionHeartbeat": "heartbeat",
                  "SessionStart": "session_start"}.get(ev, ev.lower() or None),
        "tool": p.get("tool_name") or p.get("tool") or "",
        "file_path": ti.get("file_path") or ti.get("filePath") or ti.get("path"),
        "command": ti.get("command") or ti.get("cmd"),
        "cwd": p.get("cwd"),
    }


def sniff_bash_writes(cmd):
    """从 shell 命令里嗅探可能的写文件目标（尽力而为，允许漏判）。"""
    targets = []
    for m in re.finditer(r">>?\s*([^\s;&|<>]+)", cmd):
        t = m.group(1)
        if t not in ("1", "2", "&1", "&2") and not t.startswith("/dev/"):
            targets.append(t)
    for m in re.finditer(r"\btee\s+(?:-\w+\s+)*([^\s;&|]+)", cmd):
        targets.append(m.group(1))
    for m in re.finditer(r"\bsed\s+(?:-[a-zA-Z]+\s+)*-i\S*\s+(?:'[^']*'|\"[^\"]*\"|\S+)\s+([^\s;&|]+)", cmd):
        targets.append(m.group(1))
    return [t for t in targets if t and not t.startswith("-")]


def rel_under(path, cwd):
    """把 hook 收到的路径转成项目相对路径；在项目外返回 None。"""
    base = cwd or os.getcwd()
    ap = path if os.path.isabs(path) else os.path.join(base, path)
    ap = os.path.normpath(ap)
    try:
        rel = os.path.relpath(ap, base)
    except ValueError:
        return None
    if rel.startswith("..") or os.path.isabs(rel):
        return None
    if rel.startswith(".bus/") or rel.startswith(".bus-peer"):
        return None
    return norm_path(rel)


def hook_block(cli, msg):
    """按 CLI 协议拦截：exit 2 + stderr；codex 额外输出 JSON。"""
    sys.stderr.write(msg + "\n")
    if cli == "codex":
        print(json.dumps({"hookSpecificOutput": {
            "hookEventName": "PreToolUse", "permissionDecision": "deny",
            "permissionDecisionReason": msg}}, ensure_ascii=False))
    sys.exit(2)


def hook_check_lock(conf, cli, rel, auto_lock):
    """rel 是项目相对路径；冲突时按协议拦截。auto_lock=True 时无冲突则自动加锁。"""
    payload = {"peer": conf["peer_id"], "path": rel, "note": "auto(hook)"}
    if not auto_lock:
        # 只检查：与我无关的冲突锁存在即拦
        ok, st = api_soft(conf, "GET", "/api/status")
        if not ok:
            return  # hub 不可达 → fail-open
        for l in st.get("locks", []):
            if l["owner"] == conf["peer_id"]:
                continue
            if paths_conflict(l["path"], rel) and \
                    (l["path"] != rel or l.get("region") is None):
                hook_block(cli, f"🔒 agent-bus: {lock_label(l)} 正被 {l['owner_name']} 持有"
                                + (f"（{l['note']}）" if l.get("note") else "")
                                + "。先 `bus locks` 查看，协调或用 `bus lock --wait` 排队。")
        return
    ok, r = api_soft(conf, "POST", "/api/lock", payload)
    if not ok:
        if "error" in r and "冲突" in str(r["error"]):
            hook_block(cli, f"🔒 agent-bus: {r['error']}。可用 `bus lock {rel} --wait` 排队。")
        if os.environ.get("BUS_HOOK_ENFORCE") == "1":
            hook_block(cli, f"🔒 agent-bus: hub 不可达且 BUS_HOOK_ENFORCE=1，拒绝写入 {rel}")
        # 默认 fail-open
        sys.exit(0)
    if r.get("waiting"):  # 理论上不传 wait 不会发生，防御
        sys.exit(0)
    sys.exit(0)


def cmd_hook(args):
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except ValueError:
        sys.exit(0)  # 解析失败 fail-open
    info = hook_normalize(args.cli, payload)
    ev = info.get("event")
    try:
        conf = load_conf()
    except SystemExit:
        sys.exit(0)  # 项目未加入总线 → 静默放行
    except Exception:
        sys.exit(0)

    if ev == "pre_tool_use":
        cwd = info.get("cwd") or os.getcwd()
        fp = info.get("file_path")
        tool = info.get("tool") or ""
        if fp:
            rel = rel_under(fp, cwd)
            if rel is None:
                sys.exit(0)
            hook_check_lock(conf, args.cli, rel, auto_lock=bool(WRITE_TOOL_RE.search(tool)))
        elif info.get("command") and BASH_TOOL_RE.search(tool):
            for t in sniff_bash_writes(info["command"]):
                rel = rel_under(t, cwd)
                if rel:
                    hook_check_lock(conf, args.cli, rel, auto_lock=False)
        sys.exit(0)

    if ev in ("stop", "heartbeat", "session_start"):
        ok, r = api_soft(conf, "POST", "/api/heartbeat",
                         {"peer": conf["peer_id"]}, timeout=5)
        if not ok:
            sys.exit(0)
        parts = []
        for it in r.get("blocking_items", []):
            parts.append(f"  ⛔ 阻塞私聊 {it.get('thread') or ''} ← {it.get('from')}: {it.get('body')}")
        for it in r.get("handoff_items", []):
            parts.append(f"  📦 待接收交接 {it['hid']}「{it['claim']}」← {it.get('from')}")
        for it in r.get("normal_items", []):
            parts.append(f"  📨 {it.get('thread') or '私聊'} ← {it.get('from')}: {it.get('body')}")
        if not parts:
            sys.exit(0)
        hint = ("先执行 `bus sync` 并处理（reply/resolve/decide/accept/reject），处理完再收工。"
                if ev == "stop" else "先 `bus sync` 处理完再开始新工作。")
        msg = "agent-bus 待处理：\n" + "\n".join(parts) + "\n" + hint
        if ev == "stop" and (r.get("unread_blocking") or r.get("handoff_offers")):
            hook_block(args.cli, "⛔ " + msg)
        print("📨 " + msg)
        sys.exit(0)

    sys.exit(0)


# ---- install-hooks：为各 CLI 生成配置/插件 ----

HOOK_CLIS = ("claude", "kimi", "codex", "opencode", "pi")
TOML_MARK_BEGIN = "# >>> agent-bus hooks >>>"
TOML_MARK_END = "# <<< agent-bus hooks <<<"


def bus_cmd():
    import shlex
    import shutil
    b = shutil.which("bus")
    if b:
        return shlex.quote(b)
    return f"{shlex.quote(sys.executable)} {shlex.quote(os.path.abspath(__file__))}"


def install_claude(scope, cmd):
    path = os.path.expanduser("~/.claude/settings.json") if scope == "global" \
        else os.path.join(".claude", "settings.json")
    data = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    hooks = data.setdefault("hooks", {})

    def is_ours(entry):
        return any("hook claude" in (h.get("command") or "")
                   for h in entry.get("hooks", []))

    wanted = {"PreToolUse": ["Edit|Write|MultiEdit|NotebookEdit", "Bash"],
              "Stop": [""],
              "SessionStart": [""]}
    for ev, matchers in wanted.items():
        lst = [e for e in hooks.get(ev, []) if not is_ours(e)]
        for matcher in matchers:
            entry = {"hooks": [{"type": "command", "command": f"{cmd} hook claude"}]}
            if matcher:
                entry["matcher"] = matcher
            lst.append(entry)
        hooks[ev] = lst
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def install_toml(path, cli, cmd, blocks):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    old = ""
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            old = f.read()
    old = re.sub(re.escape(TOML_MARK_BEGIN) + ".*?" + re.escape(TOML_MARK_END) + "\n?",
                 "", old, flags=re.S)
    body = TOML_MARK_BEGIN + "\n" + blocks.replace("{CMD}", cmd) + TOML_MARK_END + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(old.rstrip() + "\n\n" + body if old.strip() else body)
    return path


KIMI_BLOCKS = """[[hooks]]
event = "PreToolUse"
matcher = "Edit|Write|MultiEdit|NotebookEdit|Bash"
command = "{CMD} hook kimi"
timeout = 5

[[hooks]]
event = "Stop"
command = "{CMD} hook kimi"
timeout = 10

[[hooks]]
event = "SessionHeartbeat"
command = "{CMD} hook kimi"
timeout = 5

[[hooks]]
event = "SessionStart"
command = "{CMD} hook kimi"
timeout = 5
"""

CODEX_BLOCKS = """[[hooks.PreToolUse]]
matcher = "Edit|Write|Bash|shell"

[[hooks.PreToolUse.hooks]]
type = "command"
command = "{CMD} hook codex"
timeout = 5

[[hooks.Stop]]

[[hooks.Stop.hooks]]
type = "command"
command = "{CMD} hook codex"
timeout = 10

[[hooks.SessionStart]]

[[hooks.SessionStart.hooks]]
type = "command"
command = "{CMD} hook codex"
timeout = 10
"""

OPENCODE_PLUGIN = """// agent-bus opencode 插件：tool.execute.before 自动锁/拦截，session.idle 自动心跳
// 由 `bus install-hooks --cli opencode` 生成
import { spawnSync } from "node:child_process"

const BUS = {CMD_JSON} // argv 数组

function runBusHook(payload, cwd) {
  const r = spawnSync(BUS[0], [...BUS.slice(1), "hook", "opencode"], {
    input: JSON.stringify(payload),
    encoding: "utf-8",
    cwd,
    timeout: 8000,
  })
  return { code: r.status ?? 0, stderr: (r.stderr || "").trim() }
}

export const AgentBus = async ({ directory }) => {
  return {
    "tool.execute.before": async (input, output) => {
      const args = output.args || {}
      const res = runBusHook({
        event: "pre_tool_use",
        tool: input.tool,
        file_path: args.filePath || args.file_path || args.path,
        command: args.command,
        cwd: directory,
      }, directory)
      if (res.code === 2) throw new Error(res.stderr || "blocked by agent-bus")
    },
    event: async ({ event }) => {
      if (event.type === "session.idle") {
        runBusHook({ event: "heartbeat", cwd: directory }, directory)
      }
    },
  }
}
"""

PI_EXTENSION = """// agent-bus pi 扩展：tool_call 自动锁/拦截，turn_end 自动心跳
// 由 `bus install-hooks --cli pi` 生成
import { spawnSync } from "node:child_process"

const BUS = {CMD_JSON} // argv 数组

function runBusHook(payload, cwd) {
  const r = spawnSync(BUS[0], [...BUS.slice(1), "hook", "pi"], {
    input: JSON.stringify(payload),
    encoding: "utf-8",
    cwd,
    timeout: 8000,
  })
  return { code: r.status ?? 0, stderr: (r.stderr || "").trim() }
}

export default function (pi) {
  pi.on("tool_call", async (event, ctx) => {
    const input = event.input || {}
    const res = runBusHook({
      event: "pre_tool_use",
      tool: event.toolName,
      file_path: input.path || input.file_path || input.filePath,
      command: input.command,
      cwd: ctx.cwd,
    }, ctx.cwd)
    if (res.code === 2) return { block: true, reason: res.stderr || "blocked by agent-bus" }
  })
  pi.on("turn_end", async (_event, ctx) => {
    runBusHook({ event: "heartbeat", cwd: ctx.cwd }, ctx.cwd)
  })
}
"""


def cmd_install_hooks(args):
    import shlex
    cmd = bus_cmd()
    argv_json = json.dumps(shlex.split(cmd))
    results = []
    for cli in ([args.cli] if args.cli != "all" else HOOK_CLIS):
        if cli == "claude":
            results.append(install_claude(args.scope, cmd))
        elif cli == "kimi":
            results.append(install_toml(
                os.path.expanduser("~/.kimi-code/config.toml"), cli, cmd, KIMI_BLOCKS))
        elif cli == "codex":
            results.append(install_toml(
                os.path.expanduser("~/.codex/config.toml"), cli, cmd, CODEX_BLOCKS))
        elif cli == "opencode":
            path = os.path.expanduser("~/.config/opencode/plugins/agent-bus.ts") \
                if args.scope == "global" else os.path.join(".opencode", "plugins", "agent-bus.ts")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(OPENCODE_PLUGIN.replace("{CMD_JSON}", argv_json))
            results.append(path)
        elif cli == "pi":
            path = os.path.expanduser("~/.pi/agent/extensions/agent-bus/index.ts") \
                if args.scope == "global" else os.path.join(".pi", "extensions", "agent-bus", "index.ts")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write(PI_EXTENSION.replace("{CMD_JSON}", argv_json))
            results.append(path)
    print("✓ 已安装 agent-bus hooks：")
    for r in results:
        print(f"  {r}")
    print("协议：PreToolUse 自动锁/冲突拦截，Stop/idle 自动心跳与待办提醒。")
    print("hub 不可达时默认放行；BUS_HOOK_ENFORCE=1 可改为拦截。")


def main():
    ap = argparse.ArgumentParser(
        prog="bus", description="agent-bus：多端 agent 协作总线")
    ap.add_argument("--version", action="version", version=VERSION)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("serve", help="启动 hub（一般只有第一个开的一端执行）")
    p.add_argument("--dir", default=".bus", help="数据目录（默认 .bus）")
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--host", help="对外公布的 IP（默认自动探测）")
    p.set_defaults(fn=cmd_serve)

    p = sub.add_parser("join", help="加入总线")
    p.add_argument("--hub", help="http://host:port#token；缺省读 .bus/hub.json")
    p.add_argument("--name", help="本端名字（同项目多会话时必须各起一个）")
    p.add_argument("--cli", help="CLI 类型标记，如 kimi/claude/codex")
    p.set_defaults(fn=cmd_join)

    sub.add_parser("sync", help="心跳+收取：新公聊/新改动/私聊/交接/声明").set_defaults(fn=cmd_sync)
    sub.add_parser("status", help="总线全貌").set_defaults(fn=cmd_status)
    p = sub.add_parser("peers", help="查看各端与权限次序；rm <名字> 由主机移除掉线 peer")
    p.add_argument("rm", nargs="?", choices=["rm"])
    p.add_argument("name", nargs="?")
    p.set_defaults(fn=cmd_peers)
    sub.add_parser("leave", help="离开总线：释放锁、取消未决交接").set_defaults(fn=cmd_leave)

    p = sub.add_parser("lock", help="加锁：目录以 / 结尾；区域锁用 -r 起:止")
    p.add_argument("path")
    p.add_argument("-r", "--region", help="行区间，如 10:50")
    p.add_argument("--note", help="锁备注（要做什么）")
    p.add_argument("--ttl", type=float, help="租约分钟数（默认 15，操作自动续期）")
    p.add_argument("--wait", action="store_true", help="冲突时排队，释放后自动获得")
    p.set_defaults(fn=cmd_lock)

    p = sub.add_parser("unlock", help="解锁")
    p.add_argument("path", nargs="?")
    p.add_argument("-r", "--region")
    p.add_argument("--all", action="store_true", help="释放我所有的锁（交接中的除外）")
    p.add_argument("--force", action="store_true", help="强制解别人的锁（需更高权限/主机/对方掉线）")
    p.set_defaults(fn=cmd_unlock)

    sub.add_parser("locks", help="查看当前所有锁与等待队列").set_defaults(fn=cmd_locks)

    p = sub.add_parser("claim", help="声明/更新一项工作的归属")
    p.add_argument("name")
    p.add_argument("--note", help="这项工作的目标")
    p.add_argument("--scope", help="活动范围（仅声明，不排他），如 src/auth/")
    p.add_argument("--status", help="claimed/working/blocked/review")
    p.add_argument("--waiting-on", dest="waiting_on", help="被哪个 claim 阻塞")
    p.set_defaults(fn=cmd_claim)

    p = sub.add_parser("claims", help="查看工作声明")
    p.add_argument("--all", action="store_true", help="包括 done/abandoned")
    p.set_defaults(fn=cmd_claims)

    p = sub.add_parser("unclaim", help="关闭 claim（默认 done）")
    p.add_argument("name")
    p.add_argument("--abandoned", action="store_true")
    p.set_defaults(fn=cmd_unclaim)

    p = sub.add_parser("handoff", help="把工作（claim+锁+上下文）交接给另一端")
    p.add_argument("target", help="对端名字，或 anyone（开放式交接）")
    p.add_argument("claim", help="要交接的 claim 名")
    p.add_argument("--goal", help="目标（默认用 claim 的 note）")
    p.add_argument("--state", help="当前进度")
    p.add_argument("--blockers", help="逗号分隔的阻塞项")
    p.add_argument("--next", dest="next", help="下一步动作（必填）")
    p.add_argument("--patch", action="store_true", help="工作区有未提交改动时打包 diff 随交接携带")
    p.set_defaults(fn=cmd_handoff)

    p = sub.add_parser("capsule", help="查看交接 capsule 详情")
    p.add_argument("id")
    p.set_defaults(fn=cmd_capsule)

    p = sub.add_parser("accept", help="接收交接（claim 与锁原子转移）")
    p.add_argument("id")
    p.set_defaults(fn=cmd_accept)

    p = sub.add_parser("reject", help="拒绝/取消交接")
    p.add_argument("id")
    p.add_argument("reason", nargs="*")
    p.set_defaults(fn=cmd_reject)

    p = sub.add_parser("takeover", help="接管掉线方的 claim（合成事故现场 capsule）")
    p.add_argument("claim")
    p.add_argument("--reason", help="接管原因")
    p.set_defaults(fn=cmd_takeover)

    p = sub.add_parser("done", help="记录一次改动（别人 sync 可见）")
    p.add_argument("summary")
    p.add_argument("--files", help="逗号分隔的文件列表")
    p.add_argument("--commit", help="git commit（默认自动取当前 HEAD）")
    p.add_argument("--detail", help="长描述，存为 changes/ 下的 md")
    p.set_defaults(fn=cmd_done)

    p = sub.add_parser("log", help="改动历史（类似 git log）")
    p.add_argument("-n", type=int, default=20)
    p.set_defaults(fn=cmd_log)

    p = sub.add_parser("events", help="事件流（审计/回溯）")
    p.add_argument("-n", type=int, default=30)
    p.add_argument("--type", help="按类型前缀过滤，如 lock. / handoff. / claim.")
    p.set_defaults(fn=cmd_events)

    p = sub.add_parser("say", help="发消息：say all=公聊；say <名字>=私聊")
    p.add_argument("target", help="all 或对端名字")
    p.add_argument("message", nargs="+")
    p.add_argument("--blocking", action="store_true", help="阻塞型私聊（需协商出结论）")
    p.add_argument("--rounds", type=int, help=f"协商回合数（默认 {DEFAULT_ROUNDS}）")
    p.add_argument("--deadline", type=int, help=f"时限分钟（默认 {DEFAULT_DEADLINE_MIN}）")
    p.add_argument("--topic", help="话题标题")
    p.set_defaults(fn=cmd_say)

    p = sub.add_parser("reply", help="回复私聊")
    p.add_argument("thread")
    p.add_argument("message", nargs="+")
    p.set_defaults(fn=cmd_reply)

    p = sub.add_parser("resolve", help="双方达成共识，总结归档")
    p.add_argument("thread")
    p.add_argument("summary", nargs="+")
    p.set_defaults(fn=cmd_resolve)

    p = sub.add_parser("decide", help="（高权限方/主机）裁决定案")
    p.add_argument("thread")
    p.add_argument("summary", nargs="+")
    p.set_defaults(fn=cmd_decide)

    p = sub.add_parser("thread", help="查看某个私聊全文")
    p.add_argument("id")
    p.set_defaults(fn=cmd_thread)

    p = sub.add_parser("chat", help="查看公聊记录")
    p.add_argument("-n", type=int, default=30)
    p.set_defaults(fn=cmd_chat)

    p = sub.add_parser("board", help="共享黑板：无参查看；add <分区> <内容> 追加；reset 由主机重置")
    p.add_argument("board_cmd", nargs="?", choices=["add", "reset"])
    p.add_argument("section", nargs="?")
    p.add_argument("text", nargs="*")
    p.set_defaults(fn=cmd_board)

    p = sub.add_parser("archive", help="归档总线内容（黑板/公聊/私聊/改动/声明）到目录")
    p.add_argument("path")
    p.set_defaults(fn=cmd_archive)

    p = sub.add_parser("hook", help="（给各 CLI 的 hook 调用，非人用）从 stdin 读 payload")
    p.add_argument("cli", choices=list(HOOK_CLIS))
    p.set_defaults(fn=cmd_hook)

    p = sub.add_parser("install-hooks", help="为 CLI 安装自动锁/自动同步 hook")
    p.add_argument("cli", choices=list(HOOK_CLIS) + ["all"])
    p.add_argument("--scope", choices=["project", "global"], default="global",
                   help="claude/opencode/pi 支持 project 级，默认 global")
    p.set_defaults(fn=cmd_install_hooks)

    p = sub.add_parser("net", help="网络状态与 Tailscale 引导安装")
    p.add_argument("net_cmd", nargs="?", choices=["status", "setup"])
    p.set_defaults(fn=cmd_net)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
agent-bus — 多端 agent 协作总线（单文件、零依赖，Python 3.8+ 标准库）

功能：
  - peer 注册与权限次序（第一个 join 的是主机，rank 最小权限最高）
  - 多粒度锁：目录锁（src/auth/）、文件锁（a.ts）、区域锁（-r 10:50）
  - 公聊（say all）与私聊（阻塞型：回合制协商 + 高权限裁决）
  - 改动小历史（done），类似 git log 的"谁改了什么"
  - 共享黑板（board.md），结论性信息的公共维护区

用法见 README.md / SKILL.md。
"""
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

VERSION = "0.1.0"
TTL = 600                     # peer 心跳有效期（秒），超时视为掉线
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


class BusError(Exception):
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code
        self.msg = msg


# ==================== 服务端：状态与业务逻辑 ====================

class Bus:
    def __init__(self, datadir):
        self.dir = os.path.abspath(datadir)
        os.makedirs(os.path.join(self.dir, "changes"), exist_ok=True)
        self.state_path = os.path.join(self.dir, "state.json")
        self.board_path = os.path.join(self.dir, "board.md")
        self.mu = threading.RLock()
        if os.path.exists(self.state_path):
            with open(self.state_path, encoding="utf-8") as f:
                self.state = json.load(f)
        else:
            self.state = {
                "token": uuid.uuid4().hex,
                "peer_seq": 0, "msg_seq": 0, "change_seq": 0, "thread_seq": 0,
                "peers": {},    # id -> {id,name,host,cli,rank,joined_at,last_seen,cursors}
                "locks": {},    # key -> {key,path,region,owner,owner_name,owner_rank,note,since}
                "threads": {},  # id -> {id,topic,parties,blocking,rounds_left,deadline,status,messages,resolution}
                "inbox": {},    # peer_id -> [ {id,from,from_name,body,thread,blocking,ts,read} ]
                "public": [],   # [ {id,from,from_name,body,ts} ]
                "changes": [],  # [ {id,peer,name,summary,files,commit,detail_file,ts} ]
            }
        self.token = self.state["token"]
        if not os.path.exists(self.board_path):
            with open(self.board_path, "w", encoding="utf-8") as f:
                f.write("# 共享黑板\n\n> 所有端共同维护。结论性内容写这里，过程讨论去公聊/私聊。\n")

    def save(self):
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.state_path)

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
        p["last_seen"] = now()  # 任何操作都算一次心跳
        return p

    def peer_by_name(self, name):
        for p in self.state["peers"].values():
            if p["name"] == name or p["id"] == name:
                return p
        return None

    def join(self, name, host, cli):
        with self.mu:
            pid = short_id()
            rank = self.state["peer_seq"]
            self.state["peer_seq"] += 1
            self.state["peers"][pid] = {
                "id": pid, "name": name or f"agent-{rank}",
                "host": host or "?", "cli": cli or "?",
                "rank": rank, "joined_at": now(), "last_seen": now(),
                # 新端只看到 join 之后的新消息，历史用 log/chat 翻
                "cursors": {"public": self.state["msg_seq"],
                            "changes": self.state["change_seq"]},
            }
            self.save()
            return self.state["peers"][pid]

    def leave(self, pid):
        me = self.get_peer(pid)
        with self.mu:
            me["last_seen"] = 0
            released = [k for k, l in self.state["locks"].items() if l["owner"] == pid]
            for k in released:
                del self.state["locks"][k]
            self.save()
            return len(released)

    # ---- 锁 ----

    def lock(self, pid, path, region, note):
        path = norm_path(path)
        if region and not re.fullmatch(r"\d+:\d+", region):
            raise BusError(400, "region 格式应为 起始行:结束行，如 10:50")
        me = self.get_peer(pid)
        with self.mu:
            key = f"{path}|{region or ''}"
            for l in self.state["locks"].values():
                if l["owner"] == pid:
                    continue
                if paths_conflict(l["path"], path) and \
                        (l["path"] != path or region_overlap(l["region"], region)):
                    raise BusError(
                        409,
                        f"锁冲突：{l['path']}"
                        + (f"({l['region']})" if l["region"] else "")
                        + f" 正被 {l['owner_name']}(rank{l['owner_rank']}) 持有"
                        + (f"：{l['note']}" if l.get("note") else ""))
            self.state["locks"][key] = {
                "key": key, "path": path, "region": region,
                "owner": pid, "owner_name": me["name"], "owner_rank": me["rank"],
                "note": note or "", "since": now(),
            }
            self.save()

    def unlock(self, pid, path, region, force):
        path = norm_path(path)
        me = self.get_peer(pid)
        with self.mu:
            key = f"{path}|{region or ''}"
            l = self.state["locks"].get(key)
            if not l:
                raise BusError(404, f"没有找到锁：{key}")
            if l["owner"] != pid:
                if not force:
                    raise BusError(403, f"锁属于 {l['owner_name']}，确认后加 --force 强制解锁")
                owner = self.state["peers"].get(l["owner"])
                owner_dead = not owner or not self.alive(owner)
                host = self.host_peer()
                is_host = host is not None and host["id"] == pid
                if not (owner_dead or me["rank"] < l["owner_rank"] or is_host):
                    raise BusError(403, "权限不足：只有更高权限者、当前主机、或对方掉线后才能强制解锁")
            del self.state["locks"][key]
            self.save()

    def unlock_all(self, pid):
        self.get_peer(pid)
        with self.mu:
            mine = [k for k, l in self.state["locks"].items() if l["owner"] == pid]
            for k in mine:
                del self.state["locks"][k]
            self.save()
            return len(mine)

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

    def sweep(self):
        """惰性巡检：超时的阻塞私聊转入待裁决。每次请求时调用。"""
        with self.mu:
            changed = False
            for th in self.state["threads"].values():
                if th.get("blocking") and th["status"] == "open" \
                        and th.get("deadline") and now() > th["deadline"]:
                    self._escalate(th)
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

    # ---- 改动历史 ----

    def done(self, pid, summary, files, commit, detail):
        me = self.get_peer(pid)
        with self.mu:
            self.state["change_seq"] += 1
            cid = self.state["change_seq"]
            detail_file = None
            if detail:
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
            self.save()
            return cid

    # ---- 黑板 ----

    def _board_append(self, section, text):
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
        self.get_peer(pid)
        with self.mu:
            self._board_append(section, text)
            self.save()

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
                "changes": self.state["changes"][-50:],
                "public": self.state["public"][-50:],
                "threads": [{k: t[k] for k in
                             ("id", "topic", "parties", "blocking",
                              "rounds_left", "status")}
                            for t in self.state["threads"].values()],
            }

    def thread_view(self, tid):
        return self._thread(tid)

    def sync_view(self, pid):
        """收尾/开场仪式：心跳 + 新公聊 + 新改动 + 未读私聊（阻塞优先）+ 待办线程。"""
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
                    bus.lock(b["peer"], b["path"], b.get("region"), b.get("note"))
                    return self._send(200, {"ok": True})
                if method == "POST" and path == "/api/unlock":
                    if b.get("all"):
                        n = bus.unlock_all(b["peer"])
                        return self._send(200, {"ok": True, "released": n})
                    bus.unlock(b["peer"], b["path"], b.get("region"), b.get("force"))
                    return self._send(200, {"ok": True})
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
                if method == "GET" and path == "/api/sync":
                    return self._send(200, bus.sync_view(q["peer"][0]))
                if method == "GET" and path == "/api/status":
                    return self._send(200, bus.status_view())
                if method == "GET" and path == "/api/thread":
                    return self._send(200, bus.thread_view(q["id"][0]))
                if method == "GET" and path == "/api/board":
                    return self._send(200, {"board": bus.board_read()})
                return self._send(404, {"error": f"未知路由: {method} {path}"})
            except BusError as e:
                return self._send(e.code, {"error": e.msg})
            except (KeyError, ValueError, TypeError) as e:
                return self._send(400, {"error": f"参数错误: {e}"})

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


def cmd_serve(args):
    bus = Bus(args.dir)
    ip = args.host or guess_ip()
    hub_url = f"http://{ip}:{args.port}#{bus.token}"
    with open(os.path.join(bus.dir, "hub.json"), "w", encoding="utf-8") as f:
        json.dump({"url": f"http://{ip}:{args.port}", "token": bus.token}, f)
    print(f"agent-bus hub 已启动（数据目录: {bus.dir}）")
    print(f"其他端加入方式： bus join --hub '{hub_url}'")
    print(f"（若 {args.dir} 随项目 git 同步，对端也可直接 bus join 自动发现）")
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


def api(conf, method, path, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        conf["hub"] + path, data=data, method=method,
        headers={"X-Bus-Token": conf["token"],
                 "Content-Type": "application/json"})
    # hub 通常是本机/局域网地址，绕过系统代理（macOS 上 urllib 会读系统代理设置）
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode("utf-8")).get("error")
        except Exception:
            msg = str(e)
        die(msg)
    except urllib.error.URLError as e:
        die(f"连不上 hub（{conf['hub']}）：{e.reason}")


def parse_hub(s):
    """http://host:port#token → (url, token)"""
    if "#" in s:
        url, token = s.rsplit("#", 1)
        return url.rstrip("/"), token
    die("--hub 格式应为 http://host:port#token")


def cmd_join(args):
    if args.hub:
        url, token = parse_hub(args.hub)
    else:
        hj = os.path.join(".bus", "hub.json")
        if not os.path.exists(hj):
            die("缺少 --hub，且本地 .bus/hub.json 不存在（先 git pull 或向主机要 hub 地址）")
        with open(hj, encoding="utf-8") as f:
            c = json.load(f)
        url, token = c["url"], c["token"]
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
    print("  下一步：bus sync 查看公聊、改动和私聊")


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
        print(f"\n-- ⚠️ 阻塞私聊 ({len(v['unread_blocking'])})，必须处理 --")
        for m in v["unread_blocking"]:
            print(f"  [{m['thread']}] {m['from_name']}: {m['body']}")
        print("  → 用 bus thread <id> 看全文，bus reply/resolve/decide 处理")

    if v["unread_normal"]:
        print(f"\n-- 私聊 ({len(v['unread_normal'])}) --")
        for m in v["unread_normal"]:
            print(f"  [{m['thread']}] {m['from_name']}: {m['body']}")

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

    if v["locks"]:
        print(f"\n-- 当前锁 ({len(v['locks'])}) --")
        for l in v["locks"]:
            r = f"({l['region']})" if l["region"] else ""
            n = f" — {l['note']}" if l.get("note") else ""
            print(f"  {l['path']}{r} ← {l['owner_name']}{n}")

    if not any([v["new_changes"], v["new_public"], v["unread_blocking"],
                v["unread_normal"], v["my_open_threads"]]):
        print("  没有新消息。")


def cmd_status(args):
    conf = load_conf()
    v = api(conf, "GET", "/api/status")
    print("-- peers --")
    for p in v["peers"]:
        mark = "👑" if p["id"] == v["host"] else " "
        state = "在线" if p["alive"] else "掉线"
        print(f" {mark} rank{p['rank']}  {p['name']}  ({p['host']}/{p['cli']})  {state}")
    print(f"-- 锁 ({len(v['locks'])}) --")
    for l in v["locks"]:
        r = f"({l['region']})" if l["region"] else ""
        print(f"  {l['path']}{r} ← {l['owner_name']}  {l.get('note', '')}")
    print(f"-- 私聊线程 ({len(v['threads'])}) --")
    for t in v["threads"]:
        print(f"  [{t['id']}] {t['topic']}  {t['status']}")


def cmd_lock(args):
    conf = load_conf()
    api(conf, "POST", "/api/lock", {"peer": conf["peer_id"], "path": args.path,
                                    "region": args.region, "note": args.note})
    r = f"({args.region})" if args.region else ""
    print(f"✓ 已锁 {norm_path(args.path)}{r}")


def cmd_unlock(args):
    conf = load_conf()
    if args.all:
        r = api(conf, "POST", "/api/unlock", {"peer": conf["peer_id"], "all": True})
        print(f"✓ 已释放 {r['released']} 个锁")
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
        return
    for l in v["locks"]:
        r = f"({l['region']})" if l["region"] else ""
        n = f" — {l['note']}" if l.get("note") else ""
        print(f"  {l['path']}{r} ← {l['owner_name']}(rank{l['owner_rank']}){n}")


def git_head():
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or None
    except Exception:
        return None


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
    else:
        v = api(conf, "GET", "/api/board")
        print(v["board"])


def cmd_peers(args):
    conf = load_conf()
    v = api(conf, "GET", "/api/status")
    for p in v["peers"]:
        mark = "👑" if p["id"] == v["host"] else " "
        state = "在线" if p["alive"] else "掉线"
        print(f" {mark} rank{p['rank']}  {p['name']}  ({p['host']}/{p['cli']})  {state}")


def cmd_leave(args):
    conf = load_conf()
    r = api(conf, "POST", "/api/leave", {"peer": conf["peer_id"]})
    print(f"✓ 已离开总线，释放了 {r['released_locks']} 个锁")


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

    sub.add_parser("sync", help="心跳+收取：新公聊/新改动/私聊（阻塞优先）").set_defaults(fn=cmd_sync)
    sub.add_parser("status", help="总线全貌").set_defaults(fn=cmd_status)
    sub.add_parser("peers", help="查看各端与权限次序").set_defaults(fn=cmd_peers)
    sub.add_parser("leave", help="离开总线并释放自己的锁").set_defaults(fn=cmd_leave)

    p = sub.add_parser("lock", help="加锁：目录以 / 结尾；区域锁用 -r 起:止")
    p.add_argument("path")
    p.add_argument("-r", "--region", help="行区间，如 10:50")
    p.add_argument("--note", help="锁备注（要做什么）")
    p.set_defaults(fn=cmd_lock)

    p = sub.add_parser("unlock", help="解锁")
    p.add_argument("path", nargs="?")
    p.add_argument("-r", "--region")
    p.add_argument("--all", action="store_true", help="释放我所有的锁")
    p.add_argument("--force", action="store_true", help="强制解别人的锁（需更高权限/主机/对方掉线）")
    p.set_defaults(fn=cmd_unlock)

    sub.add_parser("locks", help="查看当前所有锁").set_defaults(fn=cmd_locks)

    p = sub.add_parser("done", help="记录一次改动（别人 sync 可见）")
    p.add_argument("summary")
    p.add_argument("--files", help="逗号分隔的文件列表")
    p.add_argument("--commit", help="git commit（默认自动取当前 HEAD）")
    p.add_argument("--detail", help="长描述，存为 changes/ 下的 md")
    p.set_defaults(fn=cmd_done)

    p = sub.add_parser("log", help="改动历史（类似 git log）")
    p.add_argument("-n", type=int, default=20)
    p.set_defaults(fn=cmd_log)

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

    p = sub.add_parser("board", help="共享黑板：无参查看；add <分区> <内容> 追加")
    p.add_argument("board_cmd", nargs="?", choices=["add"])
    p.add_argument("section", nargs="?")
    p.add_argument("text", nargs="*")
    p.set_defaults(fn=cmd_board)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()

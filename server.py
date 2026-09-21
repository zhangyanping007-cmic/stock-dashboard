#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股市宏观看板 · 本地数据代理服务
================================================
职责：
  1. 转发东方财富公开行情接口，规避浏览器 CORS 限制（浏览器无法直连行情接口）
  2. 提供静态文件服务（dashboard.html）
  3. 分层 TTL 缓存 + 并发拉取，降低上游压力

启动：  python server.py
访问：  http://localhost:8765

依赖：  仅 Python 3 标准库，无需 pip install
"""

import json
import os
import sys
import time
import threading
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

# Windows 控制台中文输出
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PORT = int(os.environ.get("DASHBOARD_PORT", "8765"))
ROOT = os.path.dirname(os.path.abspath(__file__))

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "*/*",
}

# ---------------- 指数配置（东财 secid） ----------------
CSI_INDEX = [
    ("1.000001", "上证指数", "000001"),
    ("0.399001", "深证成指", "399001"),
    ("0.399006", "创业板指", "399006"),
    ("1.000688", "科创50",   "000688"),
    ("1.000300", "沪深300",  "000300"),
    ("1.000905", "中证500",  "000905"),
    ("1.000510", "中证A500", "000510"),
    ("0.899050", "北证50",   "899050"),
]

GLOBAL_INDEX = [
    ("100.DJIA", "道琼斯",     "美股", "gb/zsDJIA"),
    ("100.NDX",  "纳斯达克",   "美股", "gb/zsNDX"),
    ("100.SPX",  "标普500",    "美股", "gb/zsSPX"),
    ("100.HSI",  "恒生指数",   "港股", "gb/zsHSI"),
    ("100.N225", "日经225",    "日本", "gb/zsN225"),
    ("100.KS11", "韩国KOSPI",  "韩国", "gb/zsKS11"),
    ("100.FTSE", "英国富时100", "英国", "gb/zsFTSE"),
]

EM_UT = "bd1d9ddb04089700cf9c27f6f7426281"
ZT_UT = "7eea3edcaed734bea9cbfc24409ed989"


# ---------------- 基础工具 ----------------
# 全局请求节流：上游接口对高频并发敏感，串行化并保持最小间隔可显著降低被拒率
_throttle_lock = threading.Lock()
_last_req_ts = [0.0]


def _throttle(min_interval=0.15):
    with _throttle_lock:
        wait = min_interval - (time.time() - _last_req_ts[0])
        if wait > 0:
            time.sleep(wait)
        _last_req_ts[0] = time.time()


def _decode(raw):
    for enc in ("utf-8", "gbk", "gb18030"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", "ignore")


def http_get(url, headers=None, timeout=10, retries=3):
    """发起 GET 请求并解码，带节流 + 指数退避重试"""
    last_err = None
    for attempt in range(retries + 1):
        try:
            _throttle()
            req = urllib.request.Request(url, headers=headers or UA)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            return _decode(raw)
        except Exception as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(0.35 * (attempt + 1))
    raise last_err


def http_json(url, headers=None, timeout=10, retries=3):
    """发起 GET 请求，返回解析后的 JSON"""
    txt = http_get(url, headers=headers, timeout=timeout, retries=retries)
    return json.loads(txt)


# ---------------- 缓存 ----------------
_cache = {}
_cache_lock = threading.Lock()


def cached(key, ttl, producer):
    """带 TTL 的缓存。producer 抛异常时回退到过期旧值（若有）"""
    now = time.time()
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (now - entry[0]) < ttl:
            return entry[1]
    try:
        val = producer()
    except Exception as exc:
        with _cache_lock:
            entry = _cache.get(key)
        if entry:
            print(f"  [warn] {key} 拉取失败，使用旧缓存: {exc}")
            return entry[1]
        raise
    with _cache_lock:
        _cache[key] = (now, val)
    return val


def to_num(v):
    """东财接口常用 '-' 表示无数据"""
    if v is None or v == "-" or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------- 数据采集 ----------------
def fetch_csi():
    """A股核心指数（含成交额、高低点、昨收）"""
    secids = ",".join(s[0] for s in CSI_INDEX)
    url = ("https://push2.eastmoney.com/api/qt/ulist.np/get"
           "?fltt=2&invt=2&fields=f2,f3,f4,f5,f6,f12,f14,f15,f16,f18"
           f"&secids={secids}&ut={EM_UT}")
    data = http_json(url)
    diff = (data.get("data") or {}).get("diff") or []
    by_code = {str(d.get("f12")): d for d in diff}

    out = []
    for secid, name, code in CSI_INDEX:
        d = by_code.get(code)
        if not d:
            continue
        out.append({
            "name": name,
            "code": code,
            "price": to_num(d.get("f2")),
            "pct": to_num(d.get("f3")),
            "chg": to_num(d.get("f4")),
            "high": to_num(d.get("f15")),
            "low": to_num(d.get("f16")),
            "prev": to_num(d.get("f18")),
            "amount": to_num(d.get("f6")),   # 成交额（元）
            "url": f"https://quote.eastmoney.com/zs{code}.html",
        })
    return out


def fetch_global():
    """全球核心指数"""
    secids = ",".join(g[0] for g in GLOBAL_INDEX)
    url = ("https://push2.eastmoney.com/api/qt/ulist.np/get"
           "?fltt=2&invt=2&fields=f2,f3,f4,f12,f14"
           f"&secids={secids}&ut={EM_UT}")
    data = http_json(url)
    diff = (data.get("data") or {}).get("diff") or []
    by_code = {str(d.get("f12")): d for d in diff}

    out = []
    for secid, name, region, path in GLOBAL_INDEX:
        short = secid.split(".")[1]
        d = by_code.get(short)
        if not d:
            continue
        out.append({
            "name": name,
            "region": region,
            "price": to_num(d.get("f2")),
            "pct": to_num(d.get("f3")),
            "chg": to_num(d.get("f4")),
            "url": f"https://quote.eastmoney.com/{path}.html",
        })
    return out


TENCENT_RANK = "https://proxy.finance.qq.com/cgi/cgi-bin/rank/pt/getRank"


def fetch_sector_bundle():
    """
    腾讯行业板块：一次请求取回全部 31 个行业，本地排序得到三个榜单，
    并顺带汇总涨跌家数。

    为什么不用东财 clist 接口：该接口对高频/并发调用限流明显
    （实测 50+ 次分页后会连返回空响应），腾讯此接口单次即可拿全，温和得多。
    """
    url = (f"{TENCENT_RANK}?board_type=hy&sort_type=turnover"
           "&direct=down&offset=0&count=50")
    headers = dict(UA)
    headers["Referer"] = "https://gu.qq.com/"
    data = http_json(url, headers=headers)
    rows = (data.get("data") or {}).get("rank_list") or []
    if not rows:
        raise RuntimeError("腾讯板块接口返回为空")

    items = []
    for r in rows:
        parts = str(r.get("zgb") or "").split("/")
        up_c = tot_c = None
        if len(parts) == 2:
            try:
                up_c, tot_c = int(parts[0]), int(parts[1])
            except ValueError:
                pass
        lzg = r.get("lzg") or {}
        name = (r.get("name") or "").strip()
        items.append({
            "name": name,
            "code": r.get("code"),
            "pct": to_num(r.get("zdf")),
            "flow": (to_num(r.get("zljlr")) or 0.0) * 1e4,        # 万元 -> 元
            "turnover": (to_num(r.get("turnover")) or 0.0) * 1e4,
            "upCount": up_c,
            "totalCount": tot_c,
            "downCount": (tot_c - up_c) if (up_c is not None and tot_c is not None) else None,
            "leader": lzg.get("name"),
            "leaderPct": to_num(lzg.get("zdf")),
            "url": "https://so.eastmoney.com/web/s?keyword=" + urllib.parse.quote(name),
        })

    valid = [x for x in items if x["pct"] is not None]
    hot = sorted(valid, key=lambda x: -x["pct"])[:6]
    weak = sorted(valid, key=lambda x: x["pct"])[:6]
    fund = sorted([x for x in items if x["flow"]], key=lambda x: -x["flow"])[:6]

    up_sum = sum(x["upCount"] for x in items if x["upCount"] is not None)
    tot_sum = sum(x["totalCount"] for x in items if x["totalCount"] is not None)
    breadth = None
    if tot_sum:
        breadth = {
            "up": up_sum,
            "down": tot_sum - up_sum,
            "flat": 0,
            "total": tot_sum,
            "upRatio": round(up_sum / tot_sum * 100, 1),
            "scope": "行业板块口径",
        }

    return {
        "sectors": {"hot": hot, "fund": fund, "weak": weak},
        "breadth": breadth,
        "boardCount": len(items),
    }


def _limit_pool(kind, date_str):
    """涨停/跌停池，返回总家数"""
    topic = "ZT" if kind == "up" else "DT"
    sort = "fbt%3Aasc" if kind == "up" else "fund%3Aasc"
    url = (f"https://push2ex.eastmoney.com/getTopic{topic}Pool"
           f"?ut={ZT_UT}&dpt=wz.ztzt&Pageindex=0&pagesize=1"
           f"&sort={sort}&date={date_str}")
    data = http_json(url)
    return int((data.get("data") or {}).get("tc") or 0)


def fetch_limit_counts():
    """涨停 / 跌停家数"""
    date_str = time.strftime("%Y%m%d")
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_up = ex.submit(_limit_pool, "up", date_str)
        f_dn = ex.submit(_limit_pool, "down", date_str)
        try:
            up = f_up.result()
        except Exception:
            up = None
        try:
            dn = f_dn.result()
        except Exception:
            dn = None
    return {"limitUp": up, "limitDown": dn}


def fetch_news():
    """7x24 财经快讯（带原文链接）"""
    url = ("https://newsapi.eastmoney.com/kuaixun/v1/"
           "getlist_102_ajaxResult_30_1_.html")
    headers = dict(UA)
    headers["Referer"] = "https://kuaixun.eastmoney.com/"
    txt = http_get(url, headers=headers, timeout=12)
    # 形如： var ajaxResult={...};
    if "=" in txt:
        txt = txt.split("=", 1)[1]
    txt = txt.strip().rstrip(";")
    data = json.loads(txt)

    items = []
    for it in (data.get("LivesList") or [])[:12]:
        show_time = str(it.get("showTime") or "")
        hhmm = show_time[11:16] if len(show_time) >= 16 else show_time
        items.append({
            "time": hhmm,
            "title": (it.get("title") or "").strip(),
            "digest": (it.get("digest") or "").strip(),
            "url": it.get("url_w") or it.get("url_unique") or "",
        })
    return items


# ---------------- 交易状态 ----------------
def trade_status():
    now = time.localtime()
    mins = now.tm_hour * 60 + now.tm_min
    if now.tm_wday >= 5:
        return "closed", "休市（周末）"
    if mins < 555:
        return "pre", "开盘前（9:15 集合竞价）"
    if mins < 570:
        return "pre", "集合竞价中"
    if mins < 690:
        return "open", "上午盘交易中"
    if mins < 780:
        return "break", "午间休市"
    if mins < 900:
        return "open", "下午盘交易中"
    return "closed", "已收盘"


# ---------------- 聚合接口 ----------------
def build_dashboard():
    """聚合所有数据。各子项独立容错，失败不影响整体"""
    result = {
        "ok": True,
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "errors": [],
    }
    status, statusText = trade_status()
    result["tradeStatus"] = status
    result["tradeStatusText"] = statusText

    # 实时行情（短缓存，刷新的核心）
    try:
        result["csi"] = cached("csi", 5, fetch_csi)
    except Exception as exc:
        result["csi"] = []
        result["errors"].append(f"csi: {exc}")

    try:
        result["global"] = cached("global", 20, fetch_global)
    except Exception as exc:
        result["global"] = []
        result["errors"].append(f"global: {exc}")

    # 涨跌停家数（中缓存）
    try:
        result["limit"] = cached("limit", 45, fetch_limit_counts)
    except Exception as exc:
        result["limit"] = {}
        result["errors"].append(f"limit: {exc}")

    # 板块 + 涨跌家数（腾讯行业板块，一次请求同时产出两者）
    try:
        bundle = cached("sector_bundle", 45, fetch_sector_bundle)
        result["sectors"] = bundle.get("sectors") or {}
        result["breadth"] = bundle.get("breadth")
    except Exception as exc:
        result["sectors"] = {}
        result["breadth"] = None
        result["errors"].append(f"sectors: {exc}")

    # 快讯（中缓存）
    try:
        result["news"] = cached("news", 45, fetch_news)
    except Exception as exc:
        result["news"] = []
        result["errors"].append(f"news: {exc}")

    return result


# ---------------- HTTP 服务 ----------------
class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=ROOT, **kwargs)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/api/dashboard":
            self.handle_api()
            return
        if path == "/api/health":
            self.send_json({"ok": True, "ts": time.strftime("%Y-%m-%d %H:%M:%S")})
            return
        if path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
            return
        # 根路径 -> 看板
        if path == "/":
            self.path = "/dashboard.html"
        super().do_GET()

    def handle_api(self):
        try:
            payload = build_dashboard()
        except Exception as exc:
            self.send_json({"ok": False, "error": str(exc)}, status=500)
            return
        self.send_json(payload)

    def send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass

    def end_headers(self):
        if self.path.endswith((".html", ".js", ".css")):
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        super().end_headers()

    def log_message(self, fmt, *args):
        msg = fmt % args
        if "/api/" in msg:
            return  # 静默 API 轮询日志
        sys.stdout.write(f"  {msg}\n")


def warm_up():
    """后台预热缓存，避免首次访问等待"""
    try:
        print("  预热缓存中…")
        build_dashboard()
        print("  预热完成")
    except Exception as exc:
        print(f"  预热跳过: {exc}")


def main():
    print("=" * 58)
    print("  股市宏观看板 · 本地数据代理服务")
    print("=" * 58)
    print(f"  看板地址:  http://localhost:{PORT}/dashboard.html")
    print(f"  数据接口:  http://localhost:{PORT}/api/dashboard")
    print(f"  数据来源:  东方财富公开行情接口")
    print("  按 Ctrl+C 停止服务")
    print("=" * 58)

    threading.Thread(target=warm_up, daemon=True).start()

    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  服务已停止")
        server.shutdown()


if __name__ == "__main__":
    main()

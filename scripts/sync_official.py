#!/usr/bin/env python3
"""Publish a complete, source-linked read-only feed of currently public records.

The feed is generated ONLY in the CI workspace and deployed using a GitHub
Pages deployment artifact, never committed to Git history. Each successful
run completely replaces the previous public feed. Failed/incomplete crawls
MUST NOT publish partial lists. Source has no public reliable "still missing"
status API, so the feed only asserts source-page availability.
"""
from __future__ import annotations

import argparse, concurrent.futures, datetime as dt, hashlib, json, re
import sys, threading, time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

BASE = "https://baobeihuijia.com"
HOST = "baobeihuijia.com"
KIND = {"3": "家寻宝贝", "4": "宝贝寻家"}
CASE_RE = re.compile(r"^/bbhj/contents/([34])/(\d+)\.html$")
REG_RE = re.compile(r"^\[(\d+)\]\s*(.+)$")
SENSITIVE = re.compile(r"(?:(?:\+?86[- ]?)?1[3-9]\d{9})|(?:\d{3,4}[- ]\d{7,8})|[a-zA-Z0-9_.+\-]+@[\w.\-]+\.[a-zA-Z]{2,}")
LABELS = {
    "失踪类型": "missing_type", "性别": "gender",
    "出生日期": "birth_date", "失踪时间": "missing_date",
    "失踪时身高": "height", "失踪地点": "missing_location",
    "失踪者特征描述": "feature_summary", "注册时间": "registration_date"
}
DATER = re.compile(r"(?:19\d{2}|20\d{2})(?:[-/.年]\d{1,2}(?:[-/.月]\d{1,2}日?)?)?")
_GLOBAL_LOCK = threading.Lock()
_next_request = 0.0

def allowed(url: str, *, detail=False) -> bool:
    u = urlparse(url)
    if u.scheme != "https" or u.hostname != HOST or u.port not in (None,443):
        return False
    return (bool(CASE_RE.fullmatch(u.path)) if detail else
            u.path.startswith("/bbhj/"))

def link(path: str) -> str:
    url = urljoin(BASE,path)
    if not allowed(url):
        raise ValueError("Not a trusted official HTTPS URL")
    return url

def download(path: str, *, limit=1_200_000) -> bytes:
    global _next_request
    url = link(path)
    # Whole project is source-rate-limited, including parallel workers.
    with _GLOBAL_LOCK:
        now = time.monotonic()
        wait = max(0, _next_request-now)
        _next_request = max(_next_request,now) + 0.20
    if wait:
        time.sleep(wait)
    req = Request(url, headers={
        "User-Agent":"GuiTuPublicGood/1.0 (source-attributed; +mail 30501813@qq.com)",
        "Accept":"text/html,application/xhtml+xml"
    })
    with urlopen(req,timeout=24) as response:
        if response.status != 200 or not allowed(response.geturl()):
            raise ValueError("Unexpected status or redirect")
        data = response.read(limit+1)
        if len(data) > limit:
            raise ValueError("Document too large")
        return data

def normalize(text: str, maxlen: int=120) -> str:
    s = re.sub(r"\s+"," ",text or "").strip().strip(":： ")
    s = SENSITIVE.sub("[隐藏]",s)
    for term in ("联系方式","身份证号","联系电话","联系人","手机：","微信：","QQ："):
        if term in s: s = s.split(term,1)[0].strip()
    return s[:maxlen]

def parse_list(source: bytes, category: str, page: int) -> list[dict]:
    soup = BeautifulSoup(source.decode("utf-8-sig"),"html.parser")
    records=[]
    for card in soup.select(".person-image"):
        label=card.select_one(".person-lable")
        if not label or KIND[category] not in label.get_text(" ",strip=True):
            continue
        a=card.select_one(".person-agent-info > a[href]")
        if a is None: continue
        t=REG_RE.fullmatch(a.get_text(" ",strip=True))
        href=a.get("href","")
        # Incoming link is relative official detail path, not arbitrary URL.
        dest=urlparse(link(href)).path
        m=CASE_RE.fullmatch(dest)
        if not t or not m or m.group(1)!=category: continue
        reg,name=t.groups()
        name=normalize(name,80)
        if not name: continue
        # Some source owners explicitly require non-redistribution or
        # further verification. Do not publish those records in our feed.
        if any(flag in name for flag in ("禁止转发", "需核实后反馈", "暂勿转发")):
            continue
        img=card.select_one(".img-box img")
        photo=None
        if img:
            raw=(img.get("src") or "").strip()
            if raw and not any(x in raw.lower() for x in ("placeholder","none-","default")):
                try: photo=link(raw)
                except ValueError:pass
        records.append({
            "id":category+"-"+m.group(2), "registration_id":reg,
            "title":name,"category":KIND[category],
            "url":link(href),"source":"宝贝回家寻子网",
            "source_page":page,"portrait_file":photo
        })
    return records

def clean_detail(raw: str, field: str) -> str:
    length = 240 if field=="feature_summary" else 100
    value=normalize(raw,length)
    if field=="missing_location":
        # Constrain publicly indexed location to prefecture-level district;
        # never publish building/door details from third-party sources.
        value=re.split(r"(?:\d+号|\d+栋|\d+单元|\d+室|\d+楼|门牌|详细地址)",value,1)[0]
        # Remove literal JavaScript placeholder components without altering
        # the valid province/city/district information.
        components = [part.strip() for part in re.split(r"[,，、]", value)]
        components = [part for part in components
                      if part and part.casefold() not in {"undefined", "null", "none", "nan"}]
        value="，".join(components)[:90]
    if field in ("birth_date","missing_date","registration_date"):
        m=DATER.search(value)
        return m.group() if m else ""
    if field=="gender" and value not in ("男","女","未知","不详"): return ""
    if field=="height" and not re.fullmatch(r"\d{1,3}(?:厘米|cm|CM)?",value): return ""
    return value

def parse_detail(source: bytes) -> dict | None:
    soup=BeautifulSoup(source.decode("utf-8-sig"),"html.parser")
    rows={}
    for item in soup.select("li"):
        key=item.select_one(".view-th")
        if not key: continue
        raw=normalize(key.get_text(" ",strip=True),220)
        label=next((x for x in LABELS if raw.startswith(x)),None)
        if not label:continue
        field=LABELS[label]
        value_node=item.select_one(".view-td")
        raw_value=(value_node.get_text(" ",strip=True) if value_node else "") or raw[len(label):]
        v=clean_detail(raw_value,field)
        if v:rows[field]=v
    if not rows:
        return None
    for attr,year in (("missing_date","missing_year"),("birth_date","birth_year"),("registration_date","registration_year")):
        if attr in rows:
            y=re.search(r"(19|20)\d{2}",rows[attr])
            if y: rows[year]=y.group()
    return rows

def fetch_detail(entry: dict) -> tuple[str,dict|None,str]:
    for attempt in range(2):
        try:
            url=entry["url"]
            if not allowed(url,detail=True):return entry["id"],None,"invalid"
            data=download(url)
            details=parse_detail(data)
            if details is None:
                return entry["id"],None,"removed_or_changed"
            summary = details.get("feature_summary", "")
            if any(flag in summary for flag in ("禁止转发", "请勿转发", "不得转发", "不允许转发", "需核实后反馈")):
                return entry["id"],None,"restricted"
            return entry["id"],details,"ok"
        except HTTPError as e:
            if e.code in (404,410):
                return entry["id"],None,"removed"
            if attempt==1:return entry["id"],None,"error"
        except (URLError,TimeoutError,ValueError,OSError):
            if attempt==1:return entry["id"],None,"error"
        time.sleep(0.7*(attempt+1))
    return entry["id"],None,"error"

def generate(pages: int,workers:int,max_details:int|None) -> dict:
    items={}
    page_count=0
    for category in ("3","4"):
        for page in range(1,pages+1):
            path=f"/bbhj/channels/{category}{'_'+str(page) if page>1 else ''}.html"
            blob=download(path)
            found=parse_list(blob,category,page)
            if not found:
                raise RuntimeError(f"Listing failed validation: {category}/{page}")
            page_count+=1
            for record in found:items[record["id"]]=record
            print(f"LIST {category}/{page}: {len(found)} entries",flush=True)
    if not max_details and pages>=20 and len(items)<1200:
        raise RuntimeError("Incomplete crawl rejected: unusually few entries")
    records=list(items.values())
    records.sort(key=lambda x:int(x["id"].split("-")[1]),reverse=True)
    if max_details is not None: records=records[:max_details]

    errors=0;gone=0;done=0;verified=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for rid,details,state in pool.map(fetch_detail,records):
            done+=1
            if state=="error": errors+=1
            elif state=="ok" and details:
                record=items[rid]
                record["offline_detail"]=details
                record["case_status"]="官网公开登记（状态请以原站为准）"
                verified.append(record)
            else:gone+=1
            if done%100==0:print(f"DETAILS {done}/{len(records)} ok={len(verified)} unavailable={gone} errors={errors}",flush=True)

    if errors:
        # Fail closed: never publish a half-synced feed as an authoritative new
        # source state. Source shutdown/timeout leaves the previous artifact in
        # place, which expires in the client.
        raise RuntimeError(f"Refusing partial publication: {errors} transient detail errors")
    if not max_details and pages>=20 and len(verified)<1100:
        raise RuntimeError("Too few verified public detail pages")
    generated=dt.datetime.now(dt.timezone.utc)
    expires=generated+dt.timedelta(hours=30)
    return {
        "schema_version":1,"source":BASE,"category":"原站公开寻亲登记",
        "fetched_at":generated.isoformat(timespec="seconds"),
        "snapshot_utc":generated.isoformat(timespec="seconds"),
        "expires_at":expires.isoformat(timespec="seconds"),
        "offline":False,
        "complete":max_details is None and pages>=20,
        "portrait_count":sum(bool(x.get("portrait_file")) for x in verified),
        "enriched_detail_count":len(verified),
        "records":verified
    }

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--pages",type=int,default=20)
    p.add_argument("--workers",type=int,default=4)
    p.add_argument("--max-details",type=int,default=None)
    p.add_argument("--output",type=Path,default=Path("site/data/latest.json"))
    a=p.parse_args()
    if not (1<=a.pages<=20 and 1<=a.workers<=4):p.error("pages 1..20, workers 1..4")
    payload=generate(a.pages,a.workers,a.max_details)
    a.output.parent.mkdir(parents=True,exist_ok=True)
    serialized=json.dumps(payload,ensure_ascii=False,separators=(",",":"),sort_keys=True).encode("utf-8")
    tmp=a.output.with_suffix(".tmp")
    tmp.write_bytes(serialized)
    tmp.replace(a.output)
    print("PUBLISHED_LOCAL",len(payload["records"]),"PUBLIC_PHOTOS",payload["portrait_count"],
          "BYTES",len(serialized),"HASH",hashlib.sha256(serialized).hexdigest()[:16])
    (a.output.parent.parent/"index.html").write_text(
        '<meta charset="utf-8"><title>宝贝归途 · 数据同步状态</title>'
        '<p>来源：原站公开寻亲档案；不代表当前实时失踪状态。</p>'
        '<p>更新时间：'+payload["fetched_at"]+'</p>'
        '<p>如需更正或撤回，请联系 30501813@qq.com。</p>',encoding="utf-8"
    )

if __name__=="__main__":
    main()

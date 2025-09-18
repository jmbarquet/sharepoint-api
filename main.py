import os, io, re
from typing import List, Optional, Tuple
from urllib.parse import urlparse, quote, unquote
from urllib.parse import unquote, quote

import requests
import pandas as pd
from unidecode import unidecode
import msal

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

# ============================ FastAPI app ============================
app = FastAPI(title="SharePoint Commissions API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten to your Bubble domain(s) in production
    allow_methods=["*"],
    allow_headers=["*"],
)

# Optional API key (set API_KEY in App Service -> Configuration)
API_KEY = os.getenv("API_KEY")
def require_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Invalid API key")

# Env secrets for Graph
TENANT = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
SCOPE = ["https://graph.microsoft.com/.default"]

# ============================ Auth & HTTP helpers ============================
def get_access_token() -> str:
    cca = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT}",
        client_credential=CLIENT_SECRET,
    )
    result = cca.acquire_token_silent(SCOPE, account=None) or cca.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise HTTPException(500, f"Auth failed: {result.get('error_description')}")
    return result["access_token"]

def gget(url: str, token: str, **kwargs):
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    r = requests.get(url, headers=headers, **kwargs)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, f"Graph error {r.status_code}: {r.text}")
    return r

# ============================ Site & Drive helpers ============================
def get_site_id(site_url: str, token: str) -> str:
    """
    Resolve the Microsoft Graph site id for a given SharePoint site URL.
    Supports root (https://contoso.sharepoint.com) and scoped sites (/sites/... or /teams/...).
    """
    parsed = urlparse(site_url)
    host = parsed.netloc
    path = parsed.path.strip("/")

    # If caller provided a scoped site path (/sites/foo or /teams/bar), resolve directly
    if path:
        try:
            j = gget(f"https://graph.microsoft.com/v1.0/sites/{host}:/{path}:/", token).json()
            if "id" in j:
                return j["id"]
        except HTTPException:
            pass

    # Try root of host
    try:
        j = gget(f"https://graph.microsoft.com/v1.0/sites/{host}:/", token).json()
        if "id" in j:
            return j["id"]
    except HTTPException:
        pass

    # Fallback: search by host
    j = gget(f"https://graph.microsoft.com/v1.0/sites?search={host}", token).json()
    vals = j.get("value", [])
    if vals:
        return vals[0]["id"]

    raise HTTPException(404, f"Could not resolve site id for '{site_url}'.")

def get_default_drive_id(site_id: str, token: str) -> str:
    return gget(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive", token).json()["id"]

def list_site_drives(site_id: str, token: str):
    return gget(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives?$select=id,name,webUrl",
        token
    ).json().get("value", [])


def get_item_by_path_in_drive(site_id: str, drive_id: str, rel_path: str, token: str):
    # decode and normalize
    rel_path = unquote(rel_path).replace("\\", "/").strip()
    enc = quote(rel_path).replace("%2F", "/")
    # OLD (flaky for some tenants):
    # url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{enc}"
    # NEW (reliable):
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{enc}"
    return gget(url, token).json()

def list_children_in_drive(site_id: str, drive_id: str, item_id: str, token: str):
    # include folder/file + parentReference
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/children?$select=id,name,file,folder,parentReference"
    items = []
    while url:
        j = gget(url, token).json()
        items.extend(j.get("value", []))
        url = j.get("@odata.nextLink")
    return items

def download_file_in_drive(site_id: str, drive_id: str, item_id: str, token: str) -> bytes:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/content"
    return gget(url, token).content

def _startswith_ci(s: str, prefix: str) -> bool:
    return (s or "").lower().startswith((prefix or "").lower())

def _strip_drive_prefix_if_present(path: str, drive_name: str) -> str:
    """If folder_path starts with the drive/library name, remove it."""
    dn = (drive_name or "").strip().lower()
    p = (path or "").strip()
    if _startswith_ci(p, dn + "/"):
        return p[len(dn)+1:]
    return p

def list_all_xlsx_recursive(site_id: str, drive_id: str, start_item_id: str, token: str):
    """Breadth-first traversal collecting all .xlsx under a folder."""
    results = []
    queue = [start_item_id]
    seen = set()
    while queue:
        cur = queue.pop(0)
        if cur in seen:
            continue
        seen.add(cur)
        kids = list_children_in_drive(site_id, drive_id, cur, token)
        for k in kids:
            if k.get("folder"):
                queue.append(k["id"])
            elif k.get("file"):
                nm = (k.get("name") or "").lower()
                if nm.endswith(".xlsx"):
                    results.append(k)
    return results

# ============================ Name/path normalization & walking ============================
def normalize_name(s: str) -> str:
    s = unidecode(s or "")
    s = s.replace("–", "-").replace("—", "-")  # smart dashes -> hyphen
    s = re.sub(r"\s+", " ", s)                # collapse spaces
    return s.strip().lower()

def find_child_by_name(children: list, target: str):
    tgt = normalize_name(target)
    for c in children:
        if normalize_name(c.get("name","")) == tgt:
            return c
    return None

def resolve_path_by_walking(site_id: str, drive_id: str, rel_path: str, token: str):
    """Walk segments with fuzzy matching (handles smart dashes, double spaces)."""
    rel_path = unquote(rel_path).replace("\\", "/").strip()
    root = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root", token).json()
    current = root
    if not rel_path:
        return current
    parts = [p for p in rel_path.split("/") if p]
    for part in parts:
        kids = list_children_in_drive(site_id, drive_id, current["id"], token)
        hit = find_child_by_name(kids, part)
        if not hit:
            return None
        current = hit
    return current

# ============================ Month parsing ============================
MONTH_MAP = {
    "jan":1,"ene":1,"feb":2,"fev":2,"mar":3,"apr":4,"abr":4,"may":5,"mai":5,"jun":6,
    "jul":7,"aug":8,"ago":8,"sep":9,"set":9,"oct":10,"out":10,"nov":11,"dec":12,"dez":12,"dic":12
}
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
BAD_HEADERS = {"nombre del agente","nome do agente","agente","agent","consultor","vendedor","cargo"}

def parse_month_from_filename(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (MonthLabel 'Aug 25', MonthDate 'YYYY-MM-01') from any ' - token - ' in the filename.
    Supports 'Ago 25', 'Abr 2025', etc.
    """
    parts = (name or "").split(" - ")
    for token in parts:
        token_wo_ext = re.sub(r"\.xlsx$", "", token, flags=re.I)
        m = re.search(r"([A-Za-zÀ-ÿ]{3,})\s*([0-9]{2,4})", token_wo_ext)
        if not m:
            continue
        mon_abbr = unidecode(m.group(1).lower())[:3]
        yy_str = m.group(2)
        mo = MONTH_MAP.get(mon_abbr)
        if not mo:
            continue
        year = int(yy_str) if len(yy_str) == 4 else 2000 + int(yy_str)
        label = f"{MONTH_NAMES[mo-1]} {str(year)[-2:]}"
        iso = f"{year:04d}-{mo:02d}-01"
        return label, iso
    return None, None

def parse_month_from_path(path_hint: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Try to infer month from the parentReference.path, e.g. '/drives/{id}/root:/.../2025/Apr'
    Handles 'Apr 2025' or adjacent '2025/Apr' / 'Apr/2025'.
    """
    if not path_hint:
        return None, None
    hint = path_hint
    if "/root:/" in hint:
        hint = hint.split("/root:/", 1)[1]
    segs = [s for s in hint.split("/") if s]

    # Adjacent segments (year near month)
    for i, seg in enumerate(segs):
        s = unidecode(seg).strip().lower()
        mon3 = s[:3]
        if mon3 in MONTH_MAP:
            neighbors = []
            if i > 0: neighbors.append(segs[i-1])
            if i+1 < len(segs): neighbors.append(segs[i+1])
            for yc in neighbors:
                ydigits = re.sub(r"[^0-9]", "", yc or "")
                if len(ydigits) in (2, 4):
                    yy = int(ydigits)
                    mo = MONTH_MAP[mon3]
                    year = yy if len(ydigits) == 4 else 2000 + yy
                    label = f"{MONTH_NAMES[mo-1]} {str(year)[-2:]}"
                    iso = f"{year:04d}-{mo:02d}-01"
                    return label, iso

    # Single segment like 'Apr 2025'
    for seg in segs:
        m = re.search(r"([A-Za-zÀ-ÿ]{3,})\s*([0-9]{2,4})", seg)
        if m:
            mon3 = unidecode(m.group(1).lower())[:3]
            mo = MONTH_MAP.get(mon3)
            if not mo:
                continue
            yy = m.group(2)
            year = int(yy) if len(yy) == 4 else 2000 + int(yy)
            label = f"{MONTH_NAMES[mo-1]} {str(year)[-2:]}"
            iso = f"{year:04d}-{mo:02d}-01"
            return label, iso

    return None, None

# ============================ Excel processing ============================
def pick_sheet_name(xl: pd.ExcelFile, desired: str):
    desired_norm = unidecode(desired or "").strip().lower()
    for s in xl.sheet_names:
        if unidecode(s).strip().lower() == desired_norm:
            return s
    for s in xl.sheet_names:
        if desired_norm and desired_norm in unidecode(s).strip().lower():
            return s
    return xl.sheet_names[0] if xl.sheet_names else None

def process_bytes(name: str, content: bytes, sheet_name: str, skip_rows: int, path_hint: Optional[str] = None) -> pd.DataFrame:
    xl = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    sheet = pick_sheet_name(xl, sheet_name)
    if not sheet:
        return pd.DataFrame()

    raw = xl.parse(sheet, header=None, skiprows=skip_rows)
    sub = raw.iloc[:, 1:7].copy()  # B..G
    sub.columns = ["AgentRaw","_PosRaw","Upsell","Total nights","Total sales","Total sales USD"]

    # Clean rows
    sub = sub[sub["AgentRaw"].notna()]
    sub["AgentRaw"] = sub["AgentRaw"].astype(str).str.strip()
    sub = sub[(sub["AgentRaw"] != "") & (~sub["AgentRaw"].str.lower().isin(BAD_HEADERS))]

    # Numbers
    for c in ["Upsell","Total nights","Total sales","Total sales USD"]:
        sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(0)

    # Month + file
    label, iso = parse_month_from_filename(name)
    if not label:
        label, iso = parse_month_from_path(path_hint)
    sub["Month"] = label
    sub["MonthDate"] = iso
    sub["SourceFile"] = name

    # Aggregate (normalize agent)
    sub["agent_key"] = sub["AgentRaw"].str.replace(r"\s+", "", regex=True).str.lower()
    agg = sub.groupby(["agent_key","Month","MonthDate"], as_index=False).agg({
        "Upsell":"sum",
        "Total nights":"sum",
        "Total sales":"sum",
        "Total sales USD":"sum",
        "SourceFile": lambda s: "; ".join(sorted(set(map(str, s))))
    })
    first_names = sub.groupby("agent_key", as_index=False)["AgentRaw"].first().rename(columns={"AgentRaw":"Agent"})
    out = first_names.merge(agg, on="agent_key", how="right").drop(columns=["agent_key"])
    out = out.sort_values(["Agent","MonthDate","Month"], na_position="last").reset_index(drop=True)
    return out

# ============================ Core worker ============================
def do_commissions(site_url: str, folder_path: str, sheet_name: str, skip_rows: int):
    token = get_access_token()
    site_id = get_site_id(site_url, token)

    # Build ordered list of drives (default first)
    default_drive_id = get_default_drive_id(site_id, token)
    drives = list_site_drives(site_id, token)
    ordered, seen = [], set()
    for d in drives:
        if d["id"] == default_drive_id and d["id"] not in seen:
            ordered.append(d); seen.add(d["id"])
    for d in drives:
        if d["id"] not in seen:
            ordered.append(d); seen.add(d["id"])

    # Resolve the folder in any drive (try as-is and without drive-name prefix; fallback to walking)
    folder_item, drive_id, tried = None, None, []
    base = (folder_path or "").strip()

    for d in ordered:
        dname = d.get("name","")
        candidates = [base, _strip_drive_prefix_if_present(base, dname)]
        # de-dupe preserving order
        cand_list, seen_c = [], set()
        for c in candidates:
            if c not in seen_c:
                cand_list.append(c); seen_c.add(c)

        for rel in cand_list:
            try:
                fi = get_item_by_path_in_drive(site_id, d["id"], rel, token)
                folder_item = fi; drive_id = d["id"]
                break
            except HTTPException:
                walked = resolve_path_by_walking(site_id, d["id"], rel, token)
                if walked:
                    folder_item = walked; drive_id = d["id"]
                    break
                tried.append(f"{dname}:{rel}")
        if folder_item is not None:
            break

    if folder_item is None:
        raise HTTPException(404, f"Folder not found. Tried: {tried}")

    # Recursively collect .xlsx files under the folder
    files = list_all_xlsx_recursive(site_id, drive_id, folder_item["id"], token)
    if not files:
        return {"rows": [], "count": 0, "message": "No .xlsx files under that folder (including subfolders)."}

    frames = []
    for f in files:
        try:
            content = download_file_in_drive(site_id, drive_id, f["id"], token)
            path_hint = (f.get("parentReference") or {}).get("path", "")
            df = process_bytes(f["name"], content, sheet_name, skip_rows, path_hint=path_hint)
            if not df.empty:
                frames.append(df)
        except Exception:
            # Skip unreadable files but continue
            continue

    if not frames:
        return {"rows": [], "count": 0, "message": "Parsed 0 rows. Check sheet_name or skip_rows."}

    df_all = pd.concat(frames, ignore_index=True).rename(columns={
        "Total nights":"Total_nights",
        "Total sales":"Total_sales",
        "Total sales USD":"Total_sales_USD"
    })
    rows = df_all.to_dict(orient="records")
    return {"rows": rows, "count": len(rows)}

# ============================ API: GET & POST ============================
class CommRequest(BaseModel):
    site_url: str = "https://incrementa993.sharepoint.com"
    folder_path: str
    sheet_name: str = "Comisiones"  # "Comissões" also works (accent-insensitive)
    skip_rows: int = 7              # data headers begin on row 8

@app.get("/", include_in_schema=False)
def root():
    return {
        "ok": True,
        "docs": "/docs",
        "health": "/healthz",
        "endpoints": [
            "/sharepoint/commissions (GET, POST)",
            "/diag/auth", "/diag/site", "/diag/drives",
            "/diag/list", "/diag/drive-root", "/diag/path", "/diag/search"
        ]
    }

@app.get("/sharepoint/commissions", dependencies=[Depends(require_key)])
def commissions_get(
    site_url: str,
    folder_path: str,
    sheet_name: str = "Comisiones",
    skip_rows: int = 7
):
    return do_commissions(site_url, folder_path, sheet_name, skip_rows)

@app.post("/sharepoint/commissions", dependencies=[Depends(require_key)])
def commissions_post(req: CommRequest):
    return do_commissions(req.site_url, req.folder_path, req.sheet_name, req.skip_rows)

# ============================ Diagnostics ============================
@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/diag/auth")
def diag_auth():
    try:
        _ = get_access_token()
        return {"token_acquired": True}
    except HTTPException as e:
        return {"token_acquired": False, "error": str(e.detail)}

@app.get("/diag/site")
def diag_site(site_url: str):
    token = get_access_token()
    try:
        sid = get_site_id(site_url, token)
        return {"site_id": sid}
    except HTTPException as e:
        return {"error": e.detail}

@app.get("/diag/drives")
def diag_drives(site_url: str):
    token = get_access_token()
    sid = get_site_id(site_url, token)
    return list_site_drives(sid, token)

# List children, optionally pin to a specific drive (name or id)
@app.get("/diag/list")
def diag_list(site_url: str, path: str = "", drive: Optional[str] = None):
    token = get_access_token()
    sid = get_site_id(site_url, token)

    default_drive_id = get_default_drive_id(sid, token)
    drives = list_site_drives(sid, token)

    if drive:
        dnorm = (drive or "").strip().lower()
        drives = [d for d in drives if d["id"] == drive or (d.get("name","").strip().lower() == dnorm)]
        if not drives:
            raise HTTPException(404, f"Drive '{drive}' not found on that site.")

    # Order default first
    ordered, seen = [], set()
    for d in drives:
        if d["id"] == default_drive_id and d["id"] not in seen:
            ordered.append(d); seen.add(d["id"])
    for d in drives:
        if d["id"] not in seen:
            ordered.append(d); seen.add(d["id"])

    path = unquote(path).replace("\\", "/").strip()
    tried = []

    # If no path, try roots of each drive until one works
    if path == "":
        for d in ordered:
            try:
                r = gget(f"https://graph.microsoft.com/v1.0/drives/{d['id']}/root/children?$select=name,id,file,folder", token).json()
                kids = r.get("value", [])
                return {"driveName": d.get("name"), "path_used": "", "children": [k["name"] for k in kids]}
            except HTTPException as e:
                tried.append({"drive": d.get("name"), "path": "", "err": e.detail})
        return {"not_found": True, "tried": tried}

    # With a path: try exact, then without drive-name prefix
    for d in ordered:
        dn = (d.get("name") or "").strip()
        candidates = [path]
        if path.lower().startswith(dn.lower() + "/"):
            candidates.append(path[len(dn)+1:])

        for rel in dict.fromkeys(candidates):
            try:
                item = get_item_by_path_in_drive(sid, d["id"], rel, token)
                kids = list_children_in_drive(sid, d["id"], item["id"], token)
                return {"driveName": d.get("name"), "path_used": rel, "children": [k["name"] for k in kids]}
            except HTTPException as e:
                tried.append({"drive": d.get("name"), "path": rel, "err": e.detail})

    return {"not_found": True, "tried": tried}

# Drive-root listing by ID
@app.get("/diag/drive-root")
def diag_drive_root(drive_id: str):
    token = get_access_token()
    r = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children?$select=name,id,file,folder", token).json()
    return {"drive_id": drive_id, "children": [x["name"] for x in r.get("value", [])]}

# Path listing pinned to a drive ID
@app.get("/diag/path")
def diag_path(drive_id: str, path: str = ""):
    token = get_access_token()
    path = unquote(path).replace("\\", "/").strip()
    tried = []

    if path == "":
        r = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children?$select=name,id,file,folder", token).json()
        return {"drive_id": drive_id, "path_used": "", "children": [x["name"] for x in r.get("value", [])]}

    # Direct path
    try:
        enc = quote(path).replace("%2F", "/")
        item = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{enc}", token).json()
        kids = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item['id']}/children?$select=name,id,file,folder", token).json().get("value", [])
        return {"drive_id": drive_id, "path_used": path, "children": [x["name"] for x in kids]}
    except HTTPException as e:
        tried.append({"path": path, "err": e.detail})

    # Walk segments (fuzzy)
    root = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root", token).json()
    current = root
    parts = [p for p in path.split("/") if p]
    for part in parts:
        r = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{current['id']}/children?$select=name,id,file,folder", token).json()
        kids = r.get("value", [])
        hit = find_child_by_name(kids, part)
        if not hit:
            return {"not_found": True, "drive_id": drive_id, "tried": tried + [{"walk_segment": part, "kids": [k["name"] for k in kids]}]}
        current = hit

    kids = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{current['id']}/children?$select=name,id,file,folder", token).json().get("value", [])
    return {"drive_id": drive_id, "path_used": path, "children": [x["name"] for x in kids]}

# Drive search (finds exact paths quickly)
@app.get("/diag/search")
def diag_search(drive_id: str, q: str):
    token = get_access_token()
    j = gget(f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/search(q='{q}')?$select=name,id,webUrl,parentReference", token).json()
    return j.get("value", [])

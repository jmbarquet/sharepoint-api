import os, io, re
from typing import List, Optional, Tuple
from urllib.parse import urlparse, quote
from urllib.parse import unquote
import requests
import pandas as pd
from unidecode import unidecode
import msal

from fastapi import FastAPI, Query, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ----------------------------- FastAPI app -----------------------------
app = FastAPI(title="SharePoint Commissions API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # set your Bubble domain for tighter security
    allow_methods=["*"],
    allow_headers=["*"],
)

# Optional API key protection: set API_KEY in App Service -> Configuration
API_KEY = os.getenv("API_KEY")
def require_key(x_api_key: str = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Invalid API key")

# Secrets for MS Graph (set in App Service -> Configuration)
TENANT = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
SCOPE = ["https://graph.microsoft.com/.default"]

# ----------------------------- Auth & HTTP helpers -----------------------------
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

# ----------------------------- Site & Drive helpers -----------------------------
def get_site_id(site_url: str, token: str) -> str:
    """Resolve a site id robustly."""
    host = urlparse(site_url).netloc

    # Try 1: direct host root
    try:
        j = gget(f"https://graph.microsoft.com/v1.0/sites/{host}:/", token).json()
        if "id" in j:
            return j["id"]
    except HTTPException:
        pass

    # Try 2: tenant root
    try:
        j = gget("https://graph.microsoft.com/v1.0/sites/root", token).json()
        if "id" in j:
            return j["id"]
    except HTTPException:
        pass

    # Try 3: search
    j = gget(f"https://graph.microsoft.com/v1.0/sites?search={host}", token).json()
    vals = j.get("value", [])
    if vals:
        return vals[0]["id"]

    raise HTTPException(404, f"Could not resolve site id for host '{host}'.")

def get_default_drive_id(site_id: str, token: str) -> str:
    return gget(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive", token).json()["id"]

def list_site_drives(site_id: str, token: str):
    return gget(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives?$select=id,name,webUrl", token).json().get("value", [])

def get_item_by_path_in_drive(site_id: str, drive_id: str, rel_path: str, token: str):
    # NEW: decode and normalize the incoming path
    rel_path = unquote(rel_path).replace("\\", "/").strip()
    enc = quote(rel_path).replace("%2F", "/")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{enc}"
    return gget(url, token).json()

def list_children_in_drive(site_id: str, drive_id: str, item_id: str, token: str):
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/children?$select=id,name,file"
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
    return s.lower().startswith(prefix.lower())

def _strip_drive_prefix_if_present(path: str, drive_name: str) -> str:
    """If folder_path starts with the drive/library name, remove it."""
    dn = drive_name.strip().lower()
    p = path.strip()
    if _startswith_ci(p, dn + "/"):
        return p[len(dn)+1:]
    return p

# ----------------------------- Parsing & processing -----------------------------
MONTH_MAP = {
    "jan":1,"ene":1,"feb":2,"fev":2,"mar":3,"apr":4,"abr":4,"may":5,"mai":5,"jun":6,
    "jul":7,"aug":8,"ago":8,"sep":9,"set":9,"oct":10,"out":10,"nov":11,"dec":12,"dez":12,"dic":12
}
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
BAD_HEADERS = {"nombre del agente","nome do agente","agente","agent","consultor","vendedor","cargo"}

def parse_month_from_filename(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (MonthLabel 'Aug 25', MonthDate 'YYYY-MM-01' ISO) from any ' - token - ' containing the month.
    Supports 'Ago 25' and 'Abr 2025'.
    """
    parts = name.split(" - ")
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

def pick_sheet_name(xl, desired: str):
    desired_norm = unidecode(desired or "").strip().lower()
    for s in xl.sheet_names:
        if unidecode(s).strip().lower() == desired_norm:
            return s
    for s in xl.sheet_names:
        if desired_norm and desired_norm in unidecode(s).strip().lower():
            return s
    return xl.sheet_names[0] if xl.sheet_names else None

def process_bytes(name: str, content: bytes, sheet_name: str, skip_rows: int) -> pd.DataFrame:
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

    # Numeric conversion
    for c in ["Upsell","Total nights","Total sales","Total sales USD"]:
        sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(0)

    # Month + file
    label, iso = parse_month_from_filename(name)
    sub["Month"] = label
    sub["MonthDate"] = iso
    sub["SourceFile"] = name

    # Normalize agent & aggregate
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

# ----------------------------- Core worker -----------------------------
def do_commissions(site_url: str, folder_path: str, sheet_name: str, skip_rows: int):
    token = get_access_token()
    site_id = get_site_id(site_url, token)

    # Build an ordered list of drives (default first, then all others)
    default_drive_id = get_default_drive_id(site_id, token)
    drives = list_site_drives(site_id, token)
    # Move default to the front
    ordered = []
    seen = set()
    for d in drives:
        if d["id"] == default_drive_id and d["id"] not in seen:
            ordered.append(d); seen.add(d["id"])
    for d in drives:
        if d["id"] not in seen:
            ordered.append(d); seen.add(d["id"])

    # Try to resolve the folder path in any drive.
    folder_item = None
    drive_id = None
    tried = []

    for d in ordered:
        dname = d.get("name","")
        candidates = []
        base = folder_path.strip()
        candidates.append(base)  # as-given
        # if path starts with drive name, also try without the drive prefix
        candidates.append(_strip_drive_prefix_if_present(base, dname))

        # de-duplicate while preserving order
        seen_c = set()
        cand_list = []
        for c in candidates:
            if c not in seen_c:
                cand_list.append(c); seen_c.add(c)

        for rel in cand_list:
            try:
                fi = get_item_by_path_in_drive(site_id, d["id"], rel, token)
                folder_item = fi
                drive_id = d["id"]
                break
            except HTTPException as e:
                tried.append(f"{dname}:{rel}")
        if folder_item is not None:
            break

    if folder_item is None:
        raise HTTPException(404, f"Folder not found. Tried: {tried}")

    children = list_children_in_drive(site_id, drive_id, folder_item["id"], token)
    files = [c for c in children if c.get("file") and c["name"].lower().endswith(".xlsx")]
    if not files:
        return {"rows": [], "count": 0, "message": "No .xlsx files in that folder."}

    frames = []
    for f in files:
        try:
            content = download_file_in_drive(site_id, drive_id, f["id"], token)
            df = process_bytes(f["name"], content, sheet_name, skip_rows)
            if not df.empty:
                frames.append(df)
        except Exception:
            # Skip unreadable files but keep processing
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

# ----------------------------- API: GET & POST -----------------------------
class CommRequest(BaseModel):
    site_url: str = "https://incrementa993.sharepoint.com"
    folder_path: str
    sheet_name: str = "Comisiones"   # "Comissões" also works (accent-insensitive)
    skip_rows: int = 7               # data headers begin on row 8

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

# ----------------------------- Diagnostics (optional) -----------------------------
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

@app.get("/diag/list")
def diag_list(site_url: str, path: str = ""):
    token = get_access_token()
    sid = get_site_id(site_url, token)

    # Order drives: default first, then others
    default_drive_id = get_default_drive_id(sid, token)
    drives = list_site_drives(sid, token)
    ordered, seen = [], set()
    for d in drives:
        if d["id"] == default_drive_id and d["id"] not in seen:
            ordered.append(d); seen.add(d["id"])
    for d in drives:
        if d["id"] not in seen:
            ordered.append(d); seen.add(d["id"])

    # normalize incoming path (handles %20 etc.)
    from urllib.parse import unquote
    path = unquote(path).replace("\\", "/").strip()

    tried = []

    # If NO path: try the root of EACH drive until one works
    if path == "":
        for d in ordered:
            try:
                r = gget(
                    f"https://graph.microsoft.com/v1.0/drives/{d['id']}/root/children?$select=name,id,file",
                    token
                ).json()
                kids = r.get("value", [])
                return {
                    "driveName": d.get("name"),
                    "path_used": "",
                    "children": [k["name"] for k in kids]
                }
            except HTTPException as e:
                tried.append({"drive": d.get("name"), "path": "", "err": e.detail})
        return {"not_found": True, "tried": tried}

    # If a path IS provided: try as-is, and (if prefixed with drive name) also without it
    for d in ordered:
        dn = (d.get("name") or "").strip()
        candidates = [path]
        if path.lower().startswith(dn.lower() + "/"):
            candidates.append(path[len(dn)+1:])

        for rel in dict.fromkeys(candidates):  # dedupe, keep order
            try:
                item = get_item_by_path_in_drive(sid, d["id"], rel, token)
                kids = list_children_in_drive(sid, d["id"], item["id"], token)
                return {
                    "driveName": d.get("name"),
                    "path_used": rel,
                    "children": [k["name"] for k in kids]
                }
            except HTTPException as e:
                tried.append({"drive": d.get("name"), "path": rel, "err": e.detail})

    return {"not_found": True, "tried": tried}


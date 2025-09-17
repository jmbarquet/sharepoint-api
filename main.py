import os, io, re, urllib.parse
from typing import List, Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
import msal
import pandas as pd
from unidecode import unidecode

# --------- CORS (so Bubble or Postman can call it) ----------
app = FastAPI(title="SharePoint Commissions API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # or set your Bubble domain only
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------- Secrets from Azure App Service -> Configuration ----------
TENANT = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
SCOPE = ["https://graph.microsoft.com/.default"]

# --------- Auth helpers ----------
def get_access_token() -> str:
    import msal
    cca = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=f"https://login.microsoftonline.com/{TENANT}",
        client_credential=CLIENT_SECRET,
    )
    result = cca.acquire_token_silent(SCOPE, account=None) \
             or cca.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise HTTPException(500, f"Auth failed: {result.get('error_description')}")
    return result["access_token"]

def gget(url: str, token: str, **kwargs):
    h = kwargs.pop("headers", {})
    h["Authorization"] = f"Bearer {token}"
    r = requests.get(url, headers=h, **kwargs)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, f"Graph error {r.status_code}: {r.text}")
    return r

# --------- Graph path helpers ----------
def get_site_id(site_url: str, token: str) -> str:
    host = urllib.parse.urlparse(site_url).netloc
    return gget(f"https://graph.microsoft.com/v1.0/sites/{host}:/", token).json()["id"]

def get_folder_item(site_id: str, folder_path: str, token: str) -> dict:
    enc = urllib.parse.quote(folder_path.strip()).replace("%2F","/")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{enc}"
    return gget(url, token).json()

def list_children(site_id: str, item_id: str, token: str) -> List[dict]:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/children?$select=id,name,file"
    items = []
    while url:
        r = gget(url, token).json()
        items.extend(r.get("value", []))
        url = r.get("@odata.nextLink")
    return items

def download_file(site_id: str, item_id: str, token: str) -> bytes:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{item_id}/content"
    return gget(url, token).content

# --------- Data processing (same logic as your PQ) ----------
MONTH_MAP = {
    "jan":1,"ene":1,"feb":2,"fev":2,"mar":3,"apr":4,"abr":4,"may":5,"mai":5,"jun":6,
    "jul":7,"aug":8,"ago":8,"sep":9,"set":9,"oct":10,"out":10,"nov":11,"dec":12,"dez":12,"dic":12
}
MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
BAD_HEADERS = { "nombre del agente","nome do agente","agente","agent","consultor","vendedor","cargo" }

def parse_month_from_filename(name: str) -> Optional[str]:
    parts = name.split(" - ")
    token = parts[1] if len(parts) >= 3 else (parts[-1] if len(parts) >= 2 else "")
    token = re.sub(r"\.xlsx$", "", token, flags=re.I)
    m = re.search(r"([A-Za-zÀ-ÿ]{3})\s*([0-9]{2,4})", token or "")
    if not m:
        return None
    mon_abbr = unidecode(m.group(1).lower())[:3]
    yy = int(m.group(2))
    mo = MONTH_MAP.get(mon_abbr)
    if not mo: 
        return None
    year = yy if yy > 99 else 2000 + yy
    return f"{MONTH_NAMES[mo-1]} {str(year)[-2:]}"

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

    sub = sub[sub["AgentRaw"].notna()]
    sub["AgentRaw"] = sub["AgentRaw"].astype(str).str.strip()
    sub = sub[(sub["AgentRaw"] != "") & (~sub["AgentRaw"].str.lower().isin(BAD_HEADERS))]
    for c in ["Upsell","Total nights","Total sales","Total sales USD"]:
        sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(0)

    sub["Month"] = parse_month_from_filename(name)
    sub["SourceFile"] = name
    sub["agent_key"] = sub["AgentRaw"].str.replace(r"\s+", "", regex=True).str.lower()

    agg = sub.groupby(["agent_key","Month"], as_index=False).agg({
        "Upsell":"sum",
        "Total nights":"sum",
        "Total sales":"sum",
        "Total sales USD":"sum",
        "SourceFile": lambda s: "; ".join(sorted(set(map(str, s))))
    })
    first_names = sub.groupby("agent_key", as_index=False)["AgentRaw"].first().rename(columns={"AgentRaw":"Agent"})
    out = first_names.merge(agg, on="agent_key", how="right").drop(columns=["agent_key"])
    out = out.sort_values(["Agent","Month"], na_position="last").reset_index(drop=True)
    return out

# --------- Core worker so GET and POST can share it ----------
def do_commissions(site_url: str, folder_path: str, sheet_name: str, skip_rows: int):
    token = get_access_token()
    site_id = get_site_id(site_url, token)

    try_paths = {folder_path.strip()}
    if not folder_path.lower().startswith("implemented programs/"):
        try_paths.add(f"Implemented Programs/{folder_path.strip()}")

    folder_item = None
    last_err = None
    for fp in try_paths:
        try:
            folder_item = get_folder_item(site_id, fp, token)
            break
        except HTTPException as e:
            last_err = e
    if folder_item is None:
        raise last_err or HTTPException(404, "Folder not found")

    children = list_children(site_id, folder_item["id"], token)
    files = [c for c in children if c.get("file") and c["name"].lower().endswith(".xlsx")]
    if not files:
        return {"rows": [], "count": 0, "debug": {"folder_path_tried": list(try_paths)}}

    frames = []
    for f in files:
        content = download_file(site_id, f["id"], token)
        df = process_bytes(f["name"], content, sheet_name, skip_rows)
        if not df.empty:
            frames.append(df)

    if not frames:
        return {"rows": [], "count": 0, "message": "No rows parsed. Check sheet_name or skip_rows."}

    df_all = pd.concat(frames, ignore_index=True).rename(columns={
        "Total nights":"Total_nights",
        "Total sales":"Total_sales",
        "Total sales USD":"Total_sales_USD"
    })
    return {"rows": df_all.to_dict(orient="records"), "count": len(df_all)}

# --------- GET (querystring) still works ---------
@app.get("/sharepoint/commissions")
def commissions_get(
    site_url: str,
    folder_path: str,
    sheet_name: str = "Comisiones",
    skip_rows: int = 7
):
    return do_commissions(site_url, folder_path, sheet_name, skip_rows)

# --------- POST (JSON body) for Bubble ---------
class CommRequest(BaseModel):
    site_url: str = "https://incrementa993.sharepoint.com"
    folder_path: str
    sheet_name: str = "Comisiones"
    skip_rows: int = 7

@app.post("/sharepoint/commissions")
def commissions_post(req: CommRequest):
    return do_commissions(req.site_url, req.folder_path, req.sheet_name, req.skip_rows)

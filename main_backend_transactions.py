import os, math, traceback, io, threading
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from hdbcli import dbapi
from dotenv import load_dotenv
from cache_manager import cache  # cache_result removed: incompatible with FastAPI route injection

load_dotenv()

app = FastAPI(title="Jivo Transaction Dashboard")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Removed GZipMiddleware to prevent potential Content-Length mismatches

# Serve static files if needed
_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

def conn():
    for ip in ['192.168.1.182', '103.89.45.192']:
        try:
            # Short timeout for faster failover
            c = dbapi.connect(address=ip, port=30015, user='DATA1', password='Jivo@1989', connectTimeout=5000)
            return c
        except Exception as e:
            print(f"Failed to connect to {ip}: {str(e)}")
    raise Exception("Could not connect to any SAP HANA IP.")

def cv(v):
    if v is None: return None
    try:
        if pd.isna(v): return None
    except: pass
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
    if isinstance(v, pd.Timestamp): return v.strftime("%Y-%m-%d")
    if hasattr(v, 'item'): return v.item()
    if isinstance(v, (int, float)): return v
    return str(v)

def q(sql):
    c = None
    try:
        c = conn()
        cur = c.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        result = []
        for row in rows:
            rec = {}
            for col, val in zip(cols, row):
                rec[col] = cv(val)
            result.append(rec)
        return result
    except:
        traceback.print_exc()
        return []
    finally:
        if c:
            try: c.close()
            except: pass

def get_base_queries(date_filter, owor_date_filter=None):
    """Returns subqueries with date filters pushed down for performance.
    owor_date_filter is used for OWOR (Production) which uses PostDate instead of DocDate.
    """
    owor_filter = owor_date_filter if owor_date_filter is not None else date_filter
    schemas = {
        'Oil': 'JIVO_OIL_HANADB',
        'Mart': 'JIVO_MART_HANADB',
        'Beverages': 'JIVO_BEVERAGES_HANADB'
    }
    
    all_queries = []
    for unit, schema in schemas.items():
        # OWTR (Transfer)
        all_queries.append(f"""SELECT '{unit}' AS "Unit", T0."DocDate" AS "RefDate", TO_VARCHAR(T0."DocDate", 'Mon') AS "Month", T1."U_DEPT" AS "Department", 'Transfer' AS "TransType", T1."USER_CODE", T1."U_NAME", T0."DocNum" AS "TransId" FROM "{schema}"."OWTR" T0 LEFT JOIN "{schema}"."OUSR" T1 ON T0."UserSign" = T1."USERID" WHERE T0."DocDate" {date_filter}""")
        # ODLN (Delivery)
        all_queries.append(f"""SELECT '{unit}', T0."DocDate", TO_VARCHAR(T0."DocDate", 'Mon'), T1."U_DEPT", 'Delivery', T1."USER_CODE", T1."U_NAME", T0."DocNum" FROM "{schema}"."ODLN" T0 LEFT JOIN "{schema}"."OUSR" T1 ON T0."UserSign" = T1."USERID" WHERE T0."DocDate" {date_filter}""")
        # OWOR (Production) — uses PostDate, NOT DocDate
        all_queries.append(f"""SELECT '{unit}', T0."PostDate", TO_VARCHAR(T0."PostDate", 'Mon'), T1."U_DEPT", 'Production', T1."USER_CODE", T1."U_NAME", T0."DocNum" FROM "{schema}"."OWOR" T0 LEFT JOIN "{schema}"."OUSR" T1 ON T0."UserSign" = T1."USERID" WHERE T0."PostDate" {owor_filter}""")
        # OINV (AR Invoice)
        all_queries.append(f"""SELECT '{unit}', T0."DocDate", TO_VARCHAR(T0."DocDate", 'Mon'), T1."U_DEPT", 'AR Invoice', T1."USER_CODE", T1."U_NAME", T0."DocNum" FROM "{schema}"."OINV" T0 LEFT JOIN "{schema}"."OUSR" T1 ON T0."UserSign" = T1."USERID" WHERE T0."DocDate" {date_filter}""")
    
    return all_queries

def fetch_parallel(days=None, date_from=None, date_to=None):
    """Fetches data from all units in parallel for maximum speed"""
    if date_from and date_to:
        # DocDate filter for all tables except OWOR
        df = f">= '{date_from}' AND T0.\"DocDate\" <= '{date_to}'"
        # PostDate filter specifically for OWOR (Production orders)
        df_owor = f">= '{date_from}' AND T0.\"PostDate\" <= '{date_to}'"
    else:
        d = days or 30
        df = f">= ADD_DAYS(CURRENT_DATE, -{d})"
        df_owor = df

    # Pass owor_date_filter directly — no brittle post-processing replace
    queries = get_base_queries(df, owor_date_filter=df_owor)

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(q, queries))
    
    # Flatten and return
    combined = []
    for r in results:
        combined.extend(r)
    print(f"DEBUG: Fetched {len(combined)} total records.")
    return combined

@app.get("/api/transactions/summary")
def get_summary(days: int = Query(30), date_from: str = Query(None), date_to: str = Query(None)):
    data = fetch_parallel(days, date_from, date_to)
    df = pd.DataFrame(data)
    if df.empty:
        return {"data": []}
    
    summary = df.groupby('Unit').agg(
        Count=('TransId', 'count'),
        Users=('USER_CODE', 'nunique')
    ).reset_index().to_dict('records')
    
    # Return plain dict — FastAPI serializes this correctly (Bug Fix #1 & #5)
    return {"data": summary}

@app.get("/api/transactions/user_performance")
def get_user_performance(days: int = Query(30), date_from: str = Query(None), date_to: str = Query(None)):
    data = fetch_parallel(days, date_from, date_to)
    print(f"DEBUG: Processing performance for {len(data)} records")
    df = pd.DataFrame(data)
    if df.empty:
        return {"data": []}
    
    perf = df.groupby(['U_NAME', 'Unit']).size().reset_index(name='EntryCount')
    perf = perf.sort_values('EntryCount', ascending=False).head(20).to_dict('records')
    
    # Return plain dict — FastAPI serializes this correctly (Bug Fix #1 & #5)
    return {"data": perf}

@app.get("/api/transactions/list")
def get_transaction_list(days: int = Query(7), date_from: str = Query(None), 
                         date_to: str = Query(None), user_name: str = Query(None), limit: int = 100):
    data = fetch_parallel(days, date_from, date_to)
    if user_name:
        data = [r for r in data if r.get('U_NAME') == user_name]
    
    # Sort and limit
    data.sort(key=lambda x: x['RefDate'] or '', reverse=True)
    return JSONResponse(content={"data": data[:limit]})

@app.get("/api/transactions/export")
def export_transactions(days: int = Query(30), date_from: str = Query(None), 
                        date_to: str = Query(None), user_name: str = Query(None)):
    data = fetch_parallel(days, date_from, date_to)
    if user_name:
        data = [r for r in data if r.get('U_NAME') == user_name]
    
    if not data:
        return JSONResponse(content={"error": "No data to export"}, status_code=400)
    
    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Transactions')
    
    output.seek(0)
    headers = {'Content-Disposition': 'attachment; filename="jivo_transactions.xlsx"'}
    return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "dashboard_transactions.html")
    with open(template_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8006)

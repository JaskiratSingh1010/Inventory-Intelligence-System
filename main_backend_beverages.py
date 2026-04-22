import os, math, traceback
import pandas as pd
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from hdbcli import dbapi

from dotenv import load_dotenv
load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.add_middleware(GZipMiddleware, minimum_size=0)

DB1 = "JIVO_MART_HANADB"
DB2 = "JIVO_OIL_HANADB"
DB3 = "JIVO_BEVERAGES_HANADB"
ALL_DBS = [DB1, DB2, DB3]
UNIT = "BEVERAGES"

# GIFT PACK excluded from Finished Goods (same pattern as Oils)
BEV_FG_VALID = ("'DRINKS','WATER','POWDER','CAPSULES'")
BEV_PM_VALID = ("'LABEL','CARTON','CAPS','SHRINK','PET BOTTLES','PREFORM',"
                "'POUCH','GLASS BOTTLES','PACKAGING MATERIAL EXPENSES','DRINKS','TAPE',"
                "'THERMOCOL','HDPE BOTTLES','PET JAR','TIKKI','CAPSULES','POWDER',"
                "'SPOON','GLUE','READY UNITS','BOTTLE HANGER'")
BEV_RM_VALID = ("'LAB','READY SYRUP','FLAVOUR','READY UNITS','SALTS','CIP',"
                "'COLOR','POWDER','SWEETNER','PULP','VITAMINS','FILTER AID',"
                "'FERTILIZER','GASES'")

GIFT_EXCL = "AND M.\"U_Sub_Group\" NOT IN ('GIFT PACK')"

# Connection settings
_CONN_PARAMS = dict(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')

def conn():
    return dbapi.connect(**_CONN_PARAMS)

def cv(v):
    if v is None: return None
    try:
        if pd.isna(v): return None
    except: pass
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
    if isinstance(v, pd.Timestamp): return v.strftime("%Y-%m-%d")
    if hasattr(v, 'item'): return v.item()
    if isinstance(v, (int, float)): return v
    # SAP HANA Decimal type - convert to float
    try:
        import decimal
        if isinstance(v, decimal.Decimal): return float(v)
    except: pass
    # Try numeric conversion for string numbers
    if isinstance(v, str):
        try:
            f = float(v)
            return int(f) if f == int(f) else f
        except: pass
        return v
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

def cf(c):
    if c and c.upper() in ('FINISHED', 'RAW MATERIAL', 'PACKAGING MATERIAL'):
        return f"AND G.\"ItmsGrpNam\"='{c.upper()}'"
    return "AND G.\"ItmsGrpNam\" IN ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL')"

def wf(w):
    return f"AND W.\"WhsCode\"='{w.replace(chr(39), chr(39)+chr(39))}'" if w else ""

def whs_f(whs):
    if not whs: return ""
    codes = ["'" + safe(c.strip()) + "'" for c in whs.split(',') if c.strip()]
    return f"AND W.\"WhsCode\" IN ({','.join(codes)})" if codes else ""

def safe(s): return s.replace("'", "''") if s else ""

def unit_f(category=None):
    """Always filter by U_Unit=BEVERAGES — confirmed: RM/PM also have this field set"""
    return f"AND M.\"U_Unit\"='{UNIT}'"

def get_dbs(category=None):
    return [DB3]  # Always only BEVERAGES DB — MART items are already dispatched/invoiced

# ════════════ KPIs ════════════
@app.get("/api/kpi")
def kpi(category: str = Query(None), whs: str = Query(None)):
    f = cf(category); uf = unit_f(category); wf_ = wf(whs)
    inv_kpi = "AND M.\"InvntItem\"='Y'"
    dbs = get_dbs(category)
    tq, tv, ts, oos = 0, 0, 0, 0
    for db in dbs:
        r = q(f"""SELECT
        ROUND(COALESCE((SELECT SUM(W."OnHand") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE 1=1 {inv_kpi} {uf} {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}),0),0) AS "TotalQty",
        ROUND(COALESCE((SELECT SUM(W."OnHand"*M."LastPurPrc") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE 1=1 {inv_kpi} {uf} {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}),0),0) AS "TotalValue",
        (SELECT COUNT(DISTINCT W."ItemCode") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE 1=1 {inv_kpi} {uf} {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}) AS "TotalSKUs",
        (SELECT COUNT(*) FROM (SELECT M."ItemCode" FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE 1=1 {inv_kpi} {uf} {f} {wf_} {GIFT_EXCL} GROUP BY M."ItemCode" HAVING SUM(W."OnHand")<=0)) AS "OutOfStockSKUs"
        FROM DUMMY""")
        if r: tq += float(r[0].get("TotalQty",0) or 0); tv += float(r[0].get("TotalValue",0) or 0); ts += float(r[0].get("TotalSKUs",0) or 0); oos += float(r[0].get("OutOfStockSKUs",0) or 0)
    return JSONResponse(content={"data": [{"TotalQty": round(tq), "TotalValue": round(tv), "TotalSKUs": ts, "OutOfStockSKUs": oos}]})

@app.get("/api/categories")
def categories(category: str = Query(None)):
    f = cf(category); uf = unit_f(category)
    dbs = get_dbs(category)
    rows = []
    for db in dbs:
        rows.extend(q(f"""SELECT G."ItmsGrpNam" AS "Category",
            COUNT(DISTINCT W."ItemCode") AS "SKUs",
            ROUND(SUM(W."OnHand"),0) AS "Qty",
            ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "Value"
            FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
            JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
            WHERE M."InvntItem"='Y' {uf} {f} AND W."OnHand">0
            GROUP BY G."ItmsGrpNam" ORDER BY "Value" DESC"""))
    agg = {}
    for r in rows:
        c = r["Category"]
        if c not in agg: agg[c] = {"Category": c, "SKUs": 0, "Qty": 0, "Value": 0}
        agg[c]["SKUs"]  += float(r.get("SKUs",  0) or 0)
        agg[c]["Qty"]   += float(r.get("Qty",   0) or 0)
        agg[c]["Value"] += float(r.get("Value", 0) or 0)
    return JSONResponse(content={"data": sorted(agg.values(), key=lambda x: x["Value"], reverse=True)})

@app.get("/api/out_of_stock")
def out_of_stock(category: str = Query(None)):
    f = cf(category); uf = unit_f(category)
    combined = []
    for db in get_dbs(category):
        combined.extend(q(f"""SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",ROUND(SUM(W."OnHand"),0) AS "TotalOnHand",M."LastPurPrc"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE M."InvntItem"='Y' {uf} {f}
        GROUP BY G."ItmsGrpNam",M."ItemCode",M."ItemName",M."LastPurPrc" HAVING SUM(W."OnHand")<=0 ORDER BY G."ItmsGrpNam",M."ItemName" """))
    return JSONResponse(content={"data": combined})

# ════════════ WAREHOUSE ════════════
@app.get("/api/warehouses")
def warehouses():
    combined = []
    for db in ALL_DBS:
        if db == DB3:
            combined.extend(q(f"""SELECT W."WhsCode", W."WhsName", '–' AS "OwnerName", '' AS "OwnerId"
            FROM {db}.OWHS W
            WHERE W."WhsCode" NOT IN ('01','BH-FA','DL-FA','GP-FA','MY-FA','DL','HR')
            ORDER BY W."WhsName" """))
        else:
            combined.extend(q(f"""SELECT W."WhsCode", W."WhsName", COALESCE(U."U_NAME",'–') AS "OwnerName", W."U_Owner" AS "OwnerId"
            FROM {db}.OWHS W LEFT JOIN {db}.OUSR U ON W."U_Owner"=U."USERID"
            WHERE W."WhsCode" NOT IN ('01','BH-FA','DL-FA','GP-FA','MY-FA','DL','HR')
            ORDER BY W."WhsName" """))
    return JSONResponse(content={"data": combined})

@app.get("/api/warehouse_owners")
def warehouse_owners():
    seen = set(); result = []
    for db in [DB1, DB2]:
        for r in q(f"""SELECT DISTINCT COALESCE(U."U_NAME",'–') AS "OwnerName" FROM {db}.OWHS H LEFT JOIN {db}.OUSR U ON H."U_Owner"=U."USERID" WHERE H."WhsCode" NOT IN ('01','BH-FA','DL-FA','GP-FA','MY-FA','DL','HR') ORDER BY "OwnerName" """):
            if r["OwnerName"] not in seen: seen.add(r["OwnerName"]); result.append(r)
    return JSONResponse(content={"data": result})

@app.get("/api/warehouse_summary")
def warehouse_summary(category: str = Query(None), owner: str = Query(None)):
    f = cf(category); uf = unit_f(category)
    combined = []
    for db in get_dbs(category):
        owner_f = f"AND U.\"U_NAME\"='{safe(owner)}'" if owner and db != DB3 else ""
        owner_sel = "COALESCE(U.\"U_NAME\",'–') AS \"OwnerName\"" if db != DB3 else "'–' AS \"OwnerName\""
        owner_join = f"LEFT JOIN {db}.OUSR U ON H.\"U_Owner\"=U.\"USERID\"" if db != DB3 else ""
        group_col = ', U."U_NAME"' if db != DB3 else ''
        
        combined.extend(q(f"""SELECT W."WhsCode", H."WhsName", {owner_sel},
        COUNT(DISTINCT W."ItemCode") AS "SKUs", ROUND(SUM(W."OnHand"),0) AS "Qty",
        ROUND(SUM(W."OnOrder"),0) AS "OnOrder",
        ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "Value"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
        JOIN {db}.OWHS H ON W."WhsCode"=H."WhsCode"
        {owner_join}
        JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE {uf[4:]} {f} AND W."OnHand">0 {owner_f}
        GROUP BY W."WhsCode",H."WhsName"{group_col} ORDER BY "Value" DESC"""))
    combined.sort(key=lambda x: x.get("Value") or 0, reverse=True)
    return JSONResponse(content={"data": combined})

@app.get("/api/warehouse_items")
def warehouse_items(whs: str = Query(""), category: str = Query(None)):
    f = cf(category); s = safe(whs)
    combined = []
    for db in ALL_DBS:
        combined.extend(q(f"""SELECT G."ItmsGrpNam" AS "Category", W."ItemCode", M."ItemName",
        COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
        ROUND(W."OnHand",0) AS "OnHand",
        ROUND(W."OnOrder",0) AS "OnOrder", ROUND(W."OnHand"-W."IsCommited"+W."OnOrder",0) AS "Available",
        ROUND(W."OnHand"*M."LastPurPrc",0) AS "StockValue"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
        JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' {f} AND W."WhsCode"='{s}' AND W."OnHand">0
        ORDER BY "StockValue" DESC"""))
    combined.sort(key=lambda x: x.get("StockValue") or 0, reverse=True)
    return JSONResponse(content={"data": combined})

@app.get("/api/stock_position")
def stock_position(category: str = Query(None), whs: str = Query(None)):
    f = cf(category); uf = unit_f(category); wf_ = wf(whs)
    combined = []
    for db in get_dbs(category):
        owner_sel = "COALESCE(U.\"U_NAME\",'–') AS \"OwnerName\"" if db != DB3 else "'–' AS \"OwnerName\""
        owner_join = f"LEFT JOIN {db}.OUSR U ON H.\"U_Owner\"=U.\"USERID\"" if db != DB3 else ""
        combined.extend(q(f"""SELECT G."ItmsGrpNam" AS "Category",W."ItemCode",M."ItemName",W."WhsCode",H."WhsName",
        {owner_sel},
        ROUND(W."OnHand",0) AS "OnHand",
        ROUND(W."OnOrder",0) AS "OnOrder", ROUND(W."OnHand"-W."IsCommited"+W."OnOrder",0) AS "Available",
        ROUND(W."OnHand"*M."LastPurPrc",0) AS "StockValue"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
        JOIN {db}.OWHS H ON W."WhsCode"=H."WhsCode"
        {owner_join}
        JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE M."U_Unit"='{UNIT}' {f} AND W."OnHand">0 {wf_}
        ORDER BY "StockValue" DESC"""))
    combined.sort(key=lambda x: x.get("StockValue") or 0, reverse=True)
    return JSONResponse(content={"data": combined})

# ════════════ MOVEMENT ════════════
@app.get("/api/movement")
def movement(days: int = Query(30), category: str = Query(None),
             date_from: str = Query(None), date_to: str = Query(None), whs: str = Query(None)):
    if days not in (7, 15, 30, 60, 90): days = 30
    f = cf(category)
    if date_from and date_to:
        date_filter = f"AND N.\"DocDate\">='{date_from}' AND N.\"DocDate\"<='{date_to}'"
    else:
        date_filter = f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    whs_f = f"AND N.\"Warehouse\"='{safe(whs)}'" if whs else ""
    combined = []
    for db in get_dbs(category):
        combined.extend(q(f"""SELECT TO_DATE(N."DocDate") AS "Date",
        N."Warehouse" AS "WhsCode", COALESCE(H."WhsName", N."Warehouse") AS "WhsName",
        M."ItemCode", M."ItemName", G."ItmsGrpNam" AS "Category",
        COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
        ROUND(SUM(N."InQty"),0) AS "InQty", ROUND(SUM(N."OutQty"),0) AS "OutQty",
        ROUND(SUM(N."InQty"*N."Price"),0) AS "InValue", ROUND(SUM(N."OutQty"*N."Price"),0) AS "OutValue"
        FROM {db}.OINM N
        JOIN {db}.OITM M ON N."ItemCode"=M."ItemCode"
        JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN {db}.OWHS H ON N."Warehouse"=H."WhsCode"
        WHERE M."U_Unit"='{UNIT}' {f} {date_filter} {whs_f}
        GROUP BY TO_DATE(N."DocDate"), N."Warehouse", H."WhsName", M."ItemCode", M."ItemName", G."ItmsGrpNam", M."U_Sub_Group"
        ORDER BY TO_DATE(N."DocDate") DESC, "OutValue" DESC"""))
    return JSONResponse(content={"data": combined})

# ════════════ MOVERS ════════════
@app.get("/api/movers_summary")
def movers_summary(days: int = Query(30), category: str = Query(None),
                   date_from: str = Query(None), date_to: str = Query(None), whs: str = Query(None)):
    f = cf(category); wf2 = whs_f(whs)
    if date_from and date_to:
        date_filter = f"AND N.\"DocDate\">='{date_from}' AND N.\"DocDate\"<='{date_to}'"
    else:
        date_filter = f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    agg = {}
    for db in get_dbs(category):
        rows = q(f"""SELECT X."MovementStatus" AS "Status",COUNT(*) AS "Count",ROUND(SUM(X."StockValue"),0) AS "Value",ROUND(SUM(X."TotalOnHand"),0) AS "Qty"
        FROM (SELECT M."ItemCode",SUM(W."OnHand") AS "TotalOnHand",SUM(W."OnHand"*M."LastPurPrc") AS "StockValue",
            CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN 'NON-MOVING' WHEN MV."TotalOut"<50 THEN 'SLOW' WHEN MV."TotalOut"<500 THEN 'MEDIUM' ELSE 'FAST' END AS "MovementStatus"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN (SELECT N."ItemCode",SUM(N."OutQty") AS "TotalOut" FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
            WHERE N."OutQty">0 {date_filter} AND I."U_Unit"='{UNIT}' GROUP BY N."ItemCode") MV ON M."ItemCode"=MV."ItemCode"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' {f} AND W."OnHand">0 {wf2} GROUP BY M."ItemCode",MV."TotalOut") X
        GROUP BY X."MovementStatus" ORDER BY CASE X."MovementStatus" WHEN 'NON-MOVING' THEN 1 WHEN 'SLOW' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END""")
        for r in rows:
            s2 = r["Status"]
            if s2 not in agg: agg[s2] = {"Status": s2, "Count": 0, "Value": 0, "Qty": 0}
            agg[s2]["Count"] += float(r.get("Count",0) or 0); agg[s2]["Value"] += float(r.get("Value",0) or 0); agg[s2]["Qty"] += float(r.get("Qty",0) or 0)
    order = ["NON-MOVING","SLOW","MEDIUM","FAST"]
    return JSONResponse(content={"data": [agg[s] for s in order if s in agg]})

@app.get("/api/movers_by_subgroup")
def movers_by_subgroup(days: int = Query(30), category: str = Query("FINISHED"),
                       date_from: str = Query(None), date_to: str = Query(None), whs: str = Query(None)):
    cat = (category or 'FINISHED').upper()
    cat_f = f"AND G.\"ItmsGrpNam\"='{cat}'"
    wf2 = whs_f(whs)
    if date_from and date_to:
        date_filter = f"AND N.\"DocDate\">='{date_from}' AND N.\"DocDate\"<='{date_to}'"
    else:
        date_filter = f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    agg = {}
    for db in get_dbs(category):
        rows = q(f"""SELECT COALESCE(M."U_Sub_Group",'UNCLASSIFIED') AS "SubGroup",
        COUNT(DISTINCT M."ItemCode") AS "TotalSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN 1 ELSE 0 END) AS "NonMovingSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>0 AND COALESCE(MV."TotalOut",0)<50 THEN 1 ELSE 0 END) AS "SlowSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>=50 AND COALESCE(MV."TotalOut",0)<500 THEN 1 ELSE 0 END) AS "MediumSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>=500 THEN 1 ELSE 0 END) AS "FastSKUs",
        ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
        ROUND(SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN W."OnHand"*M."LastPurPrc" ELSE 0 END),0) AS "StuckValue"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
        JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN (SELECT N."ItemCode",SUM(N."OutQty") AS "TotalOut" FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
            WHERE N."OutQty">0 {date_filter} AND I."U_Unit"='{UNIT}' GROUP BY N."ItemCode") MV ON M."ItemCode"=MV."ItemCode"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' {cat_f} AND W."OnHand">0 {wf2}
        GROUP BY M."U_Sub_Group" ORDER BY "StuckValue" DESC,"StockValue" DESC""")
        for r in rows:
            sg = r["SubGroup"]
            if sg not in agg:
                agg[sg] = {"SubGroup": sg, "TotalSKUs": 0, "NonMovingSKUs": 0, "SlowSKUs": 0, "MediumSKUs": 0, "FastSKUs": 0, "StockValue": 0, "StuckValue": 0}
            for k in ["TotalSKUs","NonMovingSKUs","SlowSKUs","MediumSKUs","FastSKUs","StockValue","StuckValue"]:
                agg[sg][k] += float(r.get(k, 0) or 0)
    result = sorted(agg.values(), key=lambda x: (x["StuckValue"], x["StockValue"]), reverse=True)
    return JSONResponse(content={"data": result})

@app.get("/api/movers")
def movers(days: int = Query(30), category: str = Query(None), subgroup: str = Query(None),
           status: str = Query(None), date_from: str = Query(None), date_to: str = Query(None), whs: str = Query(None)):
    f = cf(category)
    sg = f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    wf2 = whs_f(whs)
    if date_from and date_to:
        date_filter = f"AND N.\"DocDate\">='{date_from}' AND N.\"DocDate\"<='{date_to}'"
        day_div = f"(DAYS_BETWEEN(TO_DATE('{date_from}'),TO_DATE('{date_to}'))+1)"
    else:
        date_filter = f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
        day_div = str(days)
    combined = []
    for db in get_dbs(category):
        combined.extend(q(f"""SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",
        COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
        ROUND(SUM(W."OnHand"),0) AS "TotalOnHand",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
        COALESCE(MV."TotalOut",0) AS "Out{days}d",
        TO_DATE(MV."LastMoveDate") AS "LastMoveDate",
        CASE WHEN MV."LastMoveDate" IS NULL THEN -1 ELSE DAYS_BETWEEN(MV."LastMoveDate",CURRENT_DATE) END AS "DaysSinceMove",
        CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN 'NON-MOVING' WHEN MV."TotalOut"<50 THEN 'SLOW' WHEN MV."TotalOut"<500 THEN 'MEDIUM' ELSE 'FAST' END AS "MovementStatus",
        CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN -1 ELSE ROUND(SUM(W."OnHand")/(MV."TotalOut"/{day_div}),0) END AS "DaysOfStockLeft",
        STRING_AGG(W."WhsCode", ', ') AS "WhsCodes"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
        JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN (SELECT N."ItemCode",SUM(N."OutQty") AS "TotalOut",MAX(CASE WHEN N."OutQty">0 THEN N."DocDate" END) AS "LastMoveDate"
            FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
            WHERE N."OutQty">0 {date_filter} AND I."U_Unit"='{UNIT}' GROUP BY N."ItemCode") MV ON M."ItemCode"=MV."ItemCode"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' {f} AND W."OnHand">0 {wf2} {sg}
        GROUP BY G."ItmsGrpNam",M."ItemCode",M."ItemName",M."U_Sub_Group",MV."TotalOut",MV."LastMoveDate"
        ORDER BY COALESCE(MV."TotalOut",0) ASC,"StockValue" DESC"""))
    if status and status != 'all':
        combined = [r for r in combined if r.get("MovementStatus") == status.upper()]
    return JSONResponse(content={"data": combined})

# ════════════ NOT BILLED ════════════
@app.get("/api/not_billed_summary")
def not_billed_summary():
    result = []
    for period in [30, 60, 90]:
        ts, tv = 0, 0
        for db in ALL_DBS:
            r = q(f"""SELECT COUNT(DISTINCT CASE WHEN B."ItemCode" IS NULL THEN M."ItemCode" END) AS "NotBilledSKUs",
            ROUND(SUM(CASE WHEN B."ItemCode" IS NULL THEN W."OnHand"*M."LastPurPrc" ELSE 0 END),0) AS "NotBilledValue"
            FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
            LEFT JOIN (SELECT DISTINCT L."ItemCode" FROM {db}.OINV I JOIN {db}.INV1 L ON I."DocEntry"=L."DocEntry"
                WHERE I."CANCELED"='N' AND I."DocDate">=ADD_DAYS(CURRENT_DATE,-{period})) B ON M."ItemCode"=B."ItemCode"
            WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' AND G."ItmsGrpNam"='FINISHED' AND W."OnHand">0 AND M."CreateDate"<ADD_DAYS(CURRENT_DATE,-30)""")
            if r: ts += float(r[0].get("NotBilledSKUs",0) or 0); tv += float(r[0].get("NotBilledValue",0) or 0)
        result.append({"Period": f"{period} Days", "NotBilledSKUs": ts, "NotBilledValue": tv})
    return JSONResponse(content={"data": result})

@app.get("/api/not_billed_by_subgroup")
def not_billed_by_subgroup(days: int = Query(30), date_from: str = Query(None), date_to: str = Query(None), whs: str = Query(None)):
    bill_filter = f"AND I.\"DocDate\">='{date_from}' AND I.\"DocDate\"<='{date_to}'" if date_from and date_to else f"AND I.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    wf2 = whs_f(whs)
    agg = {}
    for db in ALL_DBS:
        rows = q(f"""SELECT COALESCE(M."U_Sub_Group",'UNCLASSIFIED') AS "SubGroup",
        COUNT(DISTINCT M."ItemCode") AS "TotalSKUs",
        COUNT(DISTINCT CASE WHEN B."ItemCode" IS NULL THEN M."ItemCode" END) AS "NotBilledSKUs",
        ROUND(SUM(CASE WHEN B."ItemCode" IS NULL THEN W."OnHand"*M."LastPurPrc" ELSE 0 END),0) AS "NotBilledValue",
        ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "TotalValue"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN (SELECT DISTINCT L."ItemCode" FROM {db}.OINV I JOIN {db}.INV1 L ON I."DocEntry"=L."DocEntry"
            WHERE I."CANCELED"='N' {bill_filter}) B ON M."ItemCode"=B."ItemCode"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' AND G."ItmsGrpNam"='FINISHED' AND W."OnHand">0 AND M."CreateDate"<ADD_DAYS(CURRENT_DATE,-30) {wf2}
        GROUP BY M."U_Sub_Group" ORDER BY "NotBilledValue" DESC""")
        for r in rows:
            sg = r["SubGroup"]
            if sg not in agg: agg[sg] = {"SubGroup": sg, "TotalSKUs": 0, "NotBilledSKUs": 0, "NotBilledValue": 0, "TotalValue": 0}
            for k in ["TotalSKUs","NotBilledSKUs","NotBilledValue","TotalValue"]: agg[sg][k] += float(r.get(k,0) or 0)
    return JSONResponse(content={"data": sorted(agg.values(), key=lambda x: x["NotBilledValue"], reverse=True)})

@app.get("/api/not_billed")
def not_billed(days: int = Query(30), subgroup: str = Query(None),
               date_from: str = Query(None), date_to: str = Query(None), whs: str = Query(None)):
    sg = f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    wf2 = whs_f(whs)
    if date_from and date_to:
        bill_filter = f"AND I.\"DocDate\">='{date_from}' AND I.\"DocDate\"<='{date_to}'"
        rc_filter = f"AND I.\"DocDate\">='{date_from}' AND I.\"DocDate\"<='{date_to}'"
    else:
        bill_filter = ""; rc_filter = f"AND I.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    combined = []
    for db in ALL_DBS:
        combined.extend(q(f"""SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",
        COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
        ROUND(SUM(W."OnHand"),0) AS "CurrentStock",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
        TO_DATE(LB."LastBillDate") AS "LastBillDate",
        CASE WHEN LB."LastBillDate" IS NULL THEN 'NEVER BILLED' ELSE CAST(DAYS_BETWEEN(LB."LastBillDate",CURRENT_DATE) AS VARCHAR)||' days ago' END AS "LastBilledAgo",
        LB."LastCustomer", TO_DATE(M."CreateDate") AS "CreatedOn",
        STRING_AGG(W."WhsCode", ', ') AS "WhsCodes"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN (SELECT L."ItemCode",MAX(I."DocDate") AS "LastBillDate",MAX(I."CardName") AS "LastCustomer"
            FROM {db}.OINV I JOIN {db}.INV1 L ON I."DocEntry"=L."DocEntry" WHERE I."CANCELED"='N' {bill_filter} GROUP BY L."ItemCode") LB ON M."ItemCode"=LB."ItemCode"
        LEFT JOIN (SELECT DISTINCT L."ItemCode" FROM {db}.OINV I JOIN {db}.INV1 L ON I."DocEntry"=L."DocEntry"
            WHERE I."CANCELED"='N' {rc_filter}) RC ON M."ItemCode"=RC."ItemCode"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' AND G."ItmsGrpNam"='FINISHED'
          AND W."OnHand">0 AND RC."ItemCode" IS NULL AND M."CreateDate"<ADD_DAYS(CURRENT_DATE,-30) {sg} {wf2}
        GROUP BY G."ItmsGrpNam",M."ItemCode",M."ItemName",M."U_Sub_Group",LB."LastBillDate",LB."LastCustomer",M."CreateDate"
        ORDER BY "StockValue" DESC"""))
    combined.sort(key=lambda x: x.get("StockValue") or 0, reverse=True)
    return JSONResponse(content={"data": combined})

# ════════════ ABC-XYZ ════════════
def abc_inner(db):
    return f"""SELECT M."ItemCode",M."ItemName",COALESCE(M."U_Sub_Group",'UNCLASSIFIED') AS "SubGroup",
    ROUND(SUM(W."OnHand"),0) AS "TotalOnHand",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
    ROW_NUMBER() OVER (ORDER BY SUM(W."OnHand"*M."LastPurPrc") DESC) AS "Rank",
    ROUND(SUM(SUM(W."OnHand"*M."LastPurPrc")) OVER (ORDER BY SUM(W."OnHand"*M."LastPurPrc") DESC)/NULLIF(SUM(SUM(W."OnHand"*M."LastPurPrc")) OVER(),0)*100,2) AS "CumulativePct"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' AND G."ItmsGrpNam"='FINISHED' AND W."OnHand">0
    GROUP BY M."ItemCode",M."ItemName",M."U_Sub_Group" """

def xyz_cte(db):
    return f"""MONTHLY AS (SELECT N."ItemCode",
        CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-30) THEN 'M1' WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-60) THEN 'M2' ELSE 'M3' END AS "Month",
        SUM(N."OutQty") AS "MonthlyOut"
        FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode" JOIN {db}.OITB G ON I."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE N."OutQty">0 AND N."DocDate">=ADD_DAYS(CURRENT_DATE,-90) AND I."U_Unit"='{UNIT}' AND G."ItmsGrpNam"='FINISHED'
        GROUP BY N."ItemCode",CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-30) THEN 'M1' WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-60) THEN 'M2' ELSE 'M3' END),
    STATS AS (SELECT "ItemCode",AVG("MonthlyOut") AS "AvgOut",STDDEV("MonthlyOut") AS "StdOut" FROM MONTHLY GROUP BY "ItemCode"),
    XYZ_BASE AS (SELECT S."ItemCode",ROUND(S."AvgOut",1) AS "AvgMonthlyOut",
        CASE WHEN S."AvgOut">0 THEN ROUND(S."StdOut"/S."AvgOut",4) ELSE 9999 END AS "CoV",
        CASE WHEN S."AvgOut" IS NULL OR S."AvgOut"=0 THEN 'Z' WHEN S."StdOut"/S."AvgOut"<0.5 THEN 'X' WHEN S."StdOut"/S."AvgOut"<1.0 THEN 'Y' ELSE 'Z' END AS "XYZClass"
        FROM STATS S)"""

@app.get("/api/abcxyz_summary")
def abcxyz_summary():
    agg = {}
    for db in ALL_DBS:
        AI = abc_inner(db); XC = xyz_cte(db)
        for r in q(f"""WITH ABC_BASE AS (SELECT "ItemCode","StockValue",CASE WHEN "CumulativePct"<=80 THEN 'A' WHEN "CumulativePct"<=95 THEN 'B' ELSE 'C' END AS "ABCClass" FROM ({AI}) X),
        {XC},COMBINED AS (SELECT A."ABCClass",COALESCE(X."XYZClass",'Z') AS "XYZClass",A."ABCClass"||COALESCE(X."XYZClass",'Z') AS "Combo",A."StockValue" FROM ABC_BASE A LEFT JOIN XYZ_BASE X ON A."ItemCode"=X."ItemCode")
        SELECT "Combo" AS "ABCXYZClass","ABCClass","XYZClass",COUNT(*) AS "SKUs",ROUND(SUM("StockValue"),0) AS "Value" FROM COMBINED GROUP BY "Combo","ABCClass","XYZClass" ORDER BY "ABCClass","XYZClass" """):
            k = r["ABCXYZClass"]
            if k not in agg: agg[k] = {**r, "SKUs": 0, "Value": 0}
            agg[k]["SKUs"] += float(r.get("SKUs",0) or 0); agg[k]["Value"] += float(r.get("Value",0) or 0)
    return JSONResponse(content={"data": sorted(agg.values(), key=lambda x: (x["ABCClass"],x["XYZClass"]))})

@app.get("/api/abcxyz_by_subgroup")
def abcxyz_by_subgroup():
    agg = {}
    for db in ALL_DBS:
        AI = abc_inner(db); XC = xyz_cte(db)
        for r in q(f"""WITH ABC_BASE AS (SELECT "ItemCode","ItemName","SubGroup","TotalOnHand","StockValue","CumulativePct","Rank",CASE WHEN "CumulativePct"<=80 THEN 'A' WHEN "CumulativePct"<=95 THEN 'B' ELSE 'C' END AS "ABCClass" FROM ({AI}) X),
        {XC},COMBINED AS (SELECT A."SubGroup",COUNT(*) AS "TotalSKUs",ROUND(SUM(A."StockValue"),0) AS "StockValue",
        SUM(CASE WHEN A."ABCClass"='A' THEN 1 ELSE 0 END) AS "A_Count",SUM(CASE WHEN A."ABCClass"='B' THEN 1 ELSE 0 END) AS "B_Count",SUM(CASE WHEN A."ABCClass"='C' THEN 1 ELSE 0 END) AS "C_Count",
        SUM(CASE WHEN COALESCE(X."XYZClass",'Z')='X' THEN 1 ELSE 0 END) AS "X_Count",SUM(CASE WHEN COALESCE(X."XYZClass",'Z')='Y' THEN 1 ELSE 0 END) AS "Y_Count",SUM(CASE WHEN COALESCE(X."XYZClass",'Z')='Z' THEN 1 ELSE 0 END) AS "Z_Count"
        FROM ABC_BASE A LEFT JOIN XYZ_BASE X ON A."ItemCode"=X."ItemCode" GROUP BY A."SubGroup")
        SELECT * FROM COMBINED ORDER BY "StockValue" DESC"""):
            sg = r["SubGroup"]
            if sg not in agg: agg[sg] = {k2: 0 for k2 in ["TotalSKUs","StockValue","A_Count","B_Count","C_Count","X_Count","Y_Count","Z_Count"]}; agg[sg]["SubGroup"] = sg
            for k2 in ["TotalSKUs","StockValue","A_Count","B_Count","C_Count","X_Count","Y_Count","Z_Count"]: agg[sg][k2] += float(r.get(k2,0) or 0)
    return JSONResponse(content={"data": sorted(agg.values(), key=lambda x: x["StockValue"], reverse=True)})

@app.get("/api/abcxyz")
def abcxyz(subgroup: str = Query(None), combo: str = Query(None)):
    sg = f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    combo_f = f"AND A.\"ABCClass\"||COALESCE(X.\"XYZClass\",'Z')='{safe(combo)}'" if combo and combo != 'all' else ""
    combined = []
    for db in ALL_DBS:
        AI = abc_inner(db); XC = xyz_cte(db)
        combined.extend(q(f"""WITH ABC_BASE AS (SELECT "ItemCode","ItemName","SubGroup","TotalOnHand","StockValue","CumulativePct","Rank",CASE WHEN "CumulativePct"<=80 THEN 'A' WHEN "CumulativePct"<=95 THEN 'B' ELSE 'C' END AS "ABCClass" FROM ({AI}) X),
        {XC}
        SELECT A."ItemCode",A."ItemName",A."SubGroup",A."TotalOnHand",A."StockValue",A."CumulativePct",A."Rank",A."ABCClass",
        COALESCE(X."XYZClass",'Z') AS "XYZClass",COALESCE(X."AvgMonthlyOut",0) AS "AvgMonthlyOut",COALESCE(X."CoV",9999) AS "CoV",
        A."ABCClass"||COALESCE(X."XYZClass",'Z') AS "ABCXYZClass"
        FROM ABC_BASE A LEFT JOIN XYZ_BASE X ON A."ItemCode"=X."ItemCode"
        JOIN {db}.OITM M ON A."ItemCode"=M."ItemCode"
        WHERE 1=1 {sg} {combo_f} ORDER BY A."Rank" """))
    return JSONResponse(content={"data": combined})

# ════════════ AGING ════════════
@app.get("/api/aging")
def aging(category: str = Query(None), whs: str = Query(None)):
    f = cf(category); wf2 = whs_f(whs)
    combined = []
    for db in get_dbs(category):
        combined.extend(q(f"""SELECT G."ItmsGrpNam" AS "Category",
        CASE WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=30 THEN '0-30'
             WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=60 THEN '31-60'
             WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=90 THEN '61-90'
             ELSE '90+' END AS "Bucket",
        COUNT(DISTINCT W."ItemCode") AS "Items",ROUND(SUM(W."OnHand"),0) AS "Qty",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "Value"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        JOIN (SELECT N."ItemCode",N."Warehouse",MIN(N."DocDate") AS "FirstDate" FROM {db}.OINM N WHERE N."InQty">0 GROUP BY N."ItemCode",N."Warehouse") FR
             ON W."ItemCode"=FR."ItemCode" AND W."WhsCode"=FR."Warehouse"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' {f} AND W."OnHand">0 {wf2}
        GROUP BY G."ItmsGrpNam",CASE WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=30 THEN '0-30' WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=60 THEN '31-60' WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=90 THEN '61-90' ELSE '90+' END
        ORDER BY "Category",MIN(DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE))"""))
    return JSONResponse(content={"data": combined})

@app.get("/api/aging_drill")
def aging_drill(bucket: str = Query("0-30"), category: str = Query(None), whs: str = Query(None)):
    f = cf(category); wf2 = whs_f(whs)
    lo_hi = {"0-30": (0,30), "31-60": (31,60), "61-90": (61,90), "90+": (91,99999)}
    lo, hi = lo_hi.get(bucket, (0,30))
    combined = []
    for db in get_dbs(category):
        combined.extend(q(f"""SELECT G."ItmsGrpNam" AS "Category",W."ItemCode",M."ItemName",W."WhsCode",
        TO_DATE(FR."FirstDate") AS "FirstReceiptDate",DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE) AS "DaysSitting",
        ROUND(W."OnHand",0) AS "Qty",ROUND(W."OnHand"*M."LastPurPrc",0) AS "Value"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        JOIN (SELECT N."ItemCode",N."Warehouse",MIN(N."DocDate") AS "FirstDate" FROM {db}.OINM N WHERE N."InQty">0 GROUP BY N."ItemCode",N."Warehouse") FR
             ON W."ItemCode"=FR."ItemCode" AND W."WhsCode"=FR."Warehouse"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' {f} AND W."OnHand">0 {wf2}
          AND DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)>={lo} AND DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<={hi}
        ORDER BY "Value" DESC"""))
    combined.sort(key=lambda x: x.get("Value") or 0, reverse=True)
    return JSONResponse(content={"data": combined})

# ════════════ ITEM TRACE ════════════
@app.get("/api/trace_subgroups")
def trace_subgroups(category: str = Query("FINISHED")):
    cat = (category or 'FINISHED').upper()
    dbs = [DB3] if cat in ('RAW MATERIAL', 'PACKAGING MATERIAL') else ALL_DBS
    cat_f = f"AND G.\"ItmsGrpNam\"='{cat}'" if cat in ('RAW MATERIAL','PACKAGING MATERIAL') else "AND G.\"ItmsGrpNam\"='FINISHED'"
    seen = set(); result = []
    for db in dbs:
        for r in q(f"""SELECT M."U_Sub_Group" AS "SubGroup",COUNT(DISTINCT M."ItemCode") AS "SKUs",
        ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "OnHand",ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),0) AS "StockValue"
        FROM {db}.OITM M JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN {db}.OITW W ON M."ItemCode"=W."ItemCode"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' {cat_f}
          AND M."U_Sub_Group" IS NOT NULL AND M."U_Sub_Group"!=''
        GROUP BY M."U_Sub_Group" ORDER BY SUM(W."OnHand") DESC NULLS LAST"""):
            if r["SubGroup"] not in seen: seen.add(r["SubGroup"]); result.append(r)
    return JSONResponse(content={"data": result})

@app.get("/api/trace_items")
def trace_items(category: str = Query("FINISHED"), subgroup: str = Query(None)):
    cat = (category or 'FINISHED').upper()
    dbs = [DB3] if cat in ('RAW MATERIAL', 'PACKAGING MATERIAL') else ALL_DBS
    cat_f = f"AND G.\"ItmsGrpNam\"='{cat}'" if cat in ('RAW MATERIAL','PACKAGING MATERIAL') else "AND G.\"ItmsGrpNam\"='FINISHED'"
    sg = f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    combined = []
    for db in dbs:
        combined.extend(q(f"""SELECT M."ItemCode",M."ItemName",COALESCE(M."U_Sub_Group",'–') AS "SubGroup",COALESCE(M."U_Variety",'–') AS "Variety",
        ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "OnHand",ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),0) AS "StockValue"
        FROM {db}.OITM M JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN {db}.OITW W ON M."ItemCode"=W."ItemCode"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' {cat_f} {sg}
        GROUP BY M."ItemCode",M."ItemName",M."U_Sub_Group",M."U_Variety"
        ORDER BY SUM(W."OnHand") DESC NULLS LAST,M."ItemName" """))
    return JSONResponse(content={"data": combined})

@app.get("/api/trace_header")
def trace_header(item: str = Query("")):
    s = safe(item)
    for db in ALL_DBS:
        r = q(f"""SELECT M."ItemCode",M."ItemName",TO_DATE(M."CreateDate") AS "CreateDate",TO_DATE(M."UpdateDate") AS "UpdateDate",
        M."U_Unit",M."U_Sub_Group" AS "SubGroup",M."U_Variety" AS "Variety",G."ItmsGrpNam" AS "Category",
        ROUND(M."LastPurPrc",4) AS "LastPrice",
        ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "TotalOnHand",
        ROUND(COALESCE(SUM(W."OnOrder"),0),0) AS "TotalOnOrder",ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),2) AS "StockValue"
        FROM {db}.OITM M JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN {db}.OITW W ON M."ItemCode"=W."ItemCode"
        WHERE M."ItemCode"='{s}'
        GROUP BY M."ItemCode",M."ItemName",M."CreateDate",M."UpdateDate",M."U_Unit",M."U_Sub_Group",M."U_Variety",G."ItmsGrpNam",M."LastPurPrc" """)
        if r: return JSONResponse(content={"data": r})
    return JSONResponse(content={"data": []})

@app.get("/api/trace_log")
def trace_log(item: str = Query(""), days: int = Query(0), month: str = Query(None)):
    s = safe(item)
    if month: date_f = f"AND TO_CHAR(N.\"DocDate\",'YYYY-MM')='{safe(month)}'"
    elif days > 0: date_f = f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    else: date_f = ""
    for db in ALL_DBS:
        r = q(f"""SELECT N."TransNum",N."TransType",CAST(N."BASE_REF" AS VARCHAR(50)) AS "BaseRef",
        TO_DATE(N."DocDate") AS "DocDate",N."CardName",N."JrnlMemo",N."Comments",
        ROUND(N."InQty",3) AS "InQty",ROUND(N."OutQty",3) AS "OutQty",
        ROUND(N."Price",4) AS "Price",ROUND(N."TransValue",2) AS "TransValue",
        N."Warehouse",COALESCE(H."WhsName",N."Warehouse") AS "WhsName",ROUND(N."Balance",3) AS "Balance"
        FROM {db}.OINM N LEFT JOIN {db}.OWHS H ON N."Warehouse"=H."WhsCode"
        WHERE N."ItemCode"='{s}' AND N."TransType" NOT IN (14,16) {date_f}
        ORDER BY N."DocDate" DESC,N."TransNum" DESC""")
        if r: return JSONResponse(content={"data": r})
    return JSONResponse(content={"data": []})

@app.get("/api/trace_returns")
def trace_returns(item: str = Query(""), days: int = Query(0), month: str = Query(None)):
    s = safe(item)
    if month: date_f = f"AND TO_CHAR(N.\"DocDate\",'YYYY-MM')='{safe(month)}'"
    elif days > 0: date_f = f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    else: date_f = ""
    for db in ALL_DBS:
        r = q(f"""SELECT N."TransNum",N."TransType",TO_DATE(N."DocDate") AS "DocDate",N."CardName",N."JrnlMemo",N."Comments",
        ROUND(N."InQty",3) AS "ReturnQty",ROUND(N."TransValue",2) AS "TransValue",
        N."Warehouse",COALESCE(H."WhsName",N."Warehouse") AS "WhsName",
        CASE N."TransType" WHEN 14 THEN 'AR Return' WHEN 16 THEN 'AR Credit Note' END AS "ReturnType"
        FROM {db}.OINM N LEFT JOIN {db}.OWHS H ON N."Warehouse"=H."WhsCode"
        WHERE N."ItemCode"='{s}' AND N."TransType" IN (14,16) AND N."InQty">0 {date_f}
        ORDER BY N."DocDate" DESC""")
        if r: return JSONResponse(content={"data": r})
    return JSONResponse(content={"data": []})

@app.get("/api/trace_disassembly")
def trace_disassembly(item: str = Query(""), days: int = Query(0), month: str = Query(None)):
    s = safe(item); date_f = f"AND TO_CHAR(W.\"StartDate\",'YYYY-MM')='{safe(month)}'" if month else (f"AND W.\"StartDate\">=ADD_DAYS(CURRENT_DATE,-{days})" if days > 0 else "")
    for db in ALL_DBS:
        r = q(f"""SELECT W."DocNum",W."Status",TO_DATE(W."StartDate") AS "StartDate",TO_DATE(W."DueDate") AS "DueDate",
        TO_DATE(W."CloseDate") AS "CloseDate",ROUND(W."PlannedQty",2) AS "PlannedQty",ROUND(W."CmpltQty",2) AS "ActualQty",W."Comments"
        FROM {db}.OWOR W WHERE W."ItemCode"='{s}' AND W."Type"='D' {date_f} ORDER BY W."StartDate" DESC""")
        if r: return JSONResponse(content={"data": r})
    return JSONResponse(content={"data": []})

# ════════════ PM EXTRA INFO ════════════
@app.get("/api/pm_bom")
def pm_bom(item: str = Query("")):
    s = safe(item)
    for db in ALL_DBS:
        r = q(f"""SELECT L."Father" AS "FGCode",M."ItemName" AS "FGName",
            COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
            ROUND(L."Quantity",4) AS "QtyPerUnit",
            ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "FGOnHand",
            ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),0) AS "FGStockValue"
        FROM {db}.ITT1 L
        JOIN {db}.OITM M ON L."Father"=M."ItemCode"
        LEFT JOIN {db}.OITW W ON L."Father"=W."ItemCode"
        WHERE L."Code"='{s}'
        GROUP BY L."Father",M."ItemName",M."U_Sub_Group",L."Quantity"
        ORDER BY "FGOnHand" DESC""")
        if r: return JSONResponse(content={"data": r})
    return JSONResponse(content={"data": []})

@app.get("/api/pm_consumption")
def pm_consumption(item: str = Query("")):
    s = safe(item)
    combined = []
    for db in ALL_DBS:
        combined.extend(q(f"""SELECT TO_CHAR(N."DocDate",'YYYY-MM') AS "Month",
            ROUND(SUM(CASE WHEN N."OutQty">0 THEN N."OutQty" ELSE 0 END),0) AS "ConsumedQty",
            ROUND(SUM(CASE WHEN N."InQty">0 THEN N."InQty" ELSE 0 END),0) AS "ReceivedQty",
            ROUND(SUM(CASE WHEN N."InQty">0 THEN N."InQty" ELSE 0 END)
                 -SUM(CASE WHEN N."OutQty">0 THEN N."OutQty" ELSE 0 END),0) AS "NetChange",
            ROUND(SUM(CASE WHEN N."OutQty">0 THEN N."OutQty"*N."Price" ELSE 0 END),0) AS "ConsumedValue",
            COUNT(DISTINCT TO_CHAR(N."DocDate",'YYYY-MM-DD')) AS "TxnDays"
        FROM {db}.OINM N
        WHERE N."ItemCode"='{s}'
          AND N."DocDate">=ADD_MONTHS(CURRENT_DATE,-24)
        GROUP BY TO_CHAR(N."DocDate",'YYYY-MM')
        ORDER BY "Month" DESC"""))
    agg = {}
    for r in combined:
        m = r["Month"]
        if m not in agg: agg[m] = {"Month": m, "ConsumedQty": 0, "ReceivedQty": 0, "NetChange": 0, "ConsumedValue": 0, "TxnDays": 0}
        agg[m]["ConsumedQty"] += float(r.get("ConsumedQty",0) or 0)
        agg[m]["ReceivedQty"] += float(r.get("ReceivedQty",0) or 0)
        agg[m]["NetChange"] += float(r.get("NetChange",0) or 0)
        agg[m]["ConsumedValue"] += float(r.get("ConsumedValue",0) or 0)
        agg[m]["TxnDays"] += float(r.get("TxnDays",0) or 0)
    return JSONResponse(content={"data": sorted(agg.values(), key=lambda x: x["Month"], reverse=True)})

@app.get("/api/pm_invoices")
def pm_invoices(item: str = Query("")):
    s = safe(item)
    combined = []
    for db in ALL_DBS:
        combined.extend(q(f"""SELECT I."DocNum",TO_DATE(I."DocDate") AS "DocDate",
            I."CardName" AS "Supplier",
            ROUND(L."Quantity",0) AS "Qty",
            ROUND(L."Price",4) AS "UnitPrice",
            ROUND(L."LineTotal",0) AS "LineTotal",
            CASE I."DocStatus" WHEN 'O' THEN 'Open' WHEN 'C' THEN 'Closed' ELSE I."DocStatus" END AS "Status"
        FROM {db}.OPDN I
        JOIN {db}.PDN1 L ON I."DocEntry"=L."DocEntry"
        WHERE L."ItemCode"='{s}' AND I."CANCELED"='N'
        ORDER BY I."DocDate" DESC"""))
    combined.sort(key=lambda x: x.get("DocDate") or "", reverse=True)
    return JSONResponse(content={"data": combined})

@app.get("/api/pm_summary")
def pm_summary(item:str=Query(""), period:int=Query(12)):
    s=safe(item)
    if period>0:
        before_p=f'N."DocDate" < ADD_MONTHS(CURRENT_DATE,{-period})'
        in_p=f'AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE,{-period})'
        bill_p=f'AND I."DocDate" >= ADD_MONTHS(CURRENT_DATE,{-period})'
    else:
        before_p='1=0';in_p='';bill_p=''
    agg_m={"OpeningQty":0,"OpeningValue":0,"PurchaseQty":0,"PurchaseValue":0,
            "ConsumptionQty":0,"ConsumptionValue":0,"Last3M":0,"Prev3M":0}
    agg_b={"InvoiceCount":0,"BilledQty":0,"BilledValue":0,"SupplierCount":0}
    agg_c={"ClosingQty":0,"ClosingValue":0}
    agg_w={}
    for db in ALL_DBS:
        mvt=q(f"""
        SELECT
            ROUND(SUM(CASE WHEN N."InQty">0 AND ({before_p}) THEN N."InQty" ELSE 0 END)
                 -SUM(CASE WHEN N."OutQty">0 AND ({before_p}) THEN N."OutQty" ELSE 0 END),0) AS "OpeningQty",
            ROUND(SUM(CASE WHEN N."InQty">0 AND ({before_p}) THEN N."InQty"*N."Price" ELSE 0 END)
                 -SUM(CASE WHEN N."OutQty">0 AND ({before_p}) THEN N."OutQty"*N."Price" ELSE 0 END),0) AS "OpeningValue",
            ROUND(SUM(CASE WHEN N."InQty">0 AND N."TransType" IN (20,67) {in_p} THEN N."InQty" ELSE 0 END),0) AS "PurchaseQty",
            ROUND(SUM(CASE WHEN N."InQty">0 AND N."TransType" IN (20,67) {in_p} THEN N."InQty"*N."Price" ELSE 0 END),0) AS "PurchaseValue",
            ROUND(SUM(CASE WHEN N."OutQty">0 AND N."TransType" IN (60,202) {in_p} THEN N."OutQty" ELSE 0 END),0) AS "ConsumptionQty",
            ROUND(SUM(CASE WHEN N."OutQty">0 AND N."TransType" IN (60,202) {in_p} THEN N."OutQty"*N."Price" ELSE 0 END),0) AS "ConsumptionValue",
            ROUND(SUM(CASE WHEN N."OutQty">0 AND N."TransType" IN (60,202)
                AND N."DocDate">=ADD_MONTHS(CURRENT_DATE,-3) THEN N."OutQty" ELSE 0 END),0) AS "Last3M",
            ROUND(SUM(CASE WHEN N."OutQty">0 AND N."TransType" IN (60,202)
                AND N."DocDate">=ADD_MONTHS(CURRENT_DATE,-6)
                AND N."DocDate"<ADD_MONTHS(CURRENT_DATE,-3) THEN N."OutQty" ELSE 0 END),0) AS "Prev3M"
        FROM {db}.OINM N WHERE N."ItemCode"='{s}'""")
        if mvt:
            for k in agg_m: agg_m[k]+=float(mvt[0].get(k) or 0)
        bil=q(f"""
        SELECT COALESCE(COUNT(DISTINCT I."DocNum"),0) AS "InvoiceCount",
               ROUND(COALESCE(SUM(L."Quantity"),0),0) AS "BilledQty",
               ROUND(COALESCE(SUM(L."LineTotal"),0),0) AS "BilledValue",
               COALESCE(COUNT(DISTINCT I."CardCode"),0) AS "SupplierCount"
        FROM {db}.OPCH I JOIN {db}.PCH1 L ON I."DocEntry"=L."DocEntry"
        WHERE L."ItemCode"='{s}' AND I."CANCELED"='N' {bill_p}""")
        if bil:
            for k in agg_b: agg_b[k]+=float(bil[0].get(k) or 0)
        cls=q(f"""
        SELECT ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "ClosingQty",
               ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),0) AS "ClosingValue"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
        WHERE W."ItemCode"='{s}'""")
        if cls:
            for k in agg_c: agg_c[k]+=float(cls[0].get(k) or 0)
        whs=q(f"""
        SELECT W."WhsCode", WH."WhsName", ROUND(W."OnHand",0) AS "OnHand"
        FROM {db}.OITW W JOIN {db}.OWHS WH ON W."WhsCode"=WH."WhsCode"
        WHERE W."ItemCode"='{s}' AND W."OnHand"!=0
        ORDER BY W."OnHand" DESC""")
        for row in whs:
            key=row["WhsCode"]
            if key not in agg_w:
                agg_w[key]={"WhsCode":row["WhsCode"],"WhsName":row["WhsName"],"OnHand":0.0}
            agg_w[key]["OnHand"]+=float(row.get("OnHand") or 0)
    for d in [agg_m,agg_b,agg_c]:
        for k in d: d[k]=round(d[k],0)
    warehouses=sorted([{"WhsCode":v["WhsCode"],"WhsName":v["WhsName"],"OnHand":round(v["OnHand"],0)} for v in agg_w.values() if v["OnHand"]!=0],key=lambda x:-(x["OnHand"] or 0))
    return JSONResponse(content={"movements":agg_m,"billing":agg_b,"closing":agg_c,"warehouses":warehouses})

@app.get("/api/fg_pm_summary")
def fg_pm_summary(item:str=Query(""), period:int=Query(12)):
    s=safe(item)
    if period>0:
        before_p=f'N."DocDate" < ADD_MONTHS(CURRENT_DATE,{-period})'
        in_p=f'AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE,{-period})'
    else:
        before_p='1=0';in_p=''
    combined=[]
    for db in ALL_DBS:
        r=q(f"""
        SELECT FG."FGCode",FG."FGName",FG."SubGroup",FG."QtyPerUnit",
               ROUND(SUM(CASE WHEN N."InQty">0 AND ({before_p}) THEN N."InQty" ELSE 0 END)
                    -SUM(CASE WHEN N."OutQty">0 AND ({before_p}) THEN N."OutQty" ELSE 0 END),0) AS "OpeningQty",
               ROUND(SUM(CASE WHEN N."InQty">0 {in_p} THEN N."InQty" ELSE 0 END),0) AS "ProductionQty",
               ROUND(SUM(CASE WHEN N."InQty">0 AND N."TransType"=101 {in_p} THEN N."InQty" ELSE 0 END),0) AS "ProdSpecQty",
               ROUND(SUM(CASE WHEN N."InQty">0 AND N."TransType" IN (20,67) {in_p} THEN N."InQty" ELSE 0 END),0) AS "GRQty",
               ROUND(SUM(CASE WHEN N."OutQty">0 {in_p} THEN N."OutQty" ELSE 0 END),0) AS "ARInvoiceQty",
               ROUND(SUM(CASE WHEN N."OutQty">0 AND N."TransType"=13 {in_p} THEN N."OutQty" ELSE 0 END),0) AS "ARSpecQty",
               (SELECT ROUND(COALESCE(SUM(W."OnHand"),0),0)
                FROM {db}.OITW W WHERE W."ItemCode"=FG."FGCode") AS "ClosingQty",
               (SELECT ROUND(COALESCE(SUM(W2."OnHand"*M2."LastPurPrc"),0),0)
                FROM {db}.OITW W2 JOIN {db}.OITM M2 ON W2."ItemCode"=M2."ItemCode"
                WHERE W2."ItemCode"=FG."FGCode") AS "ClosingValue"
        FROM (
            SELECT L."Father" AS "FGCode",M."ItemName" AS "FGName",
                   COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
                   ROUND(SUM(L."Quantity"),4) AS "QtyPerUnit"
            FROM {db}.ITT1 L JOIN {db}.OITM M ON L."Father"=M."ItemCode"
            WHERE L."Code"='{s}'
            GROUP BY L."Father",M."ItemName",M."U_Sub_Group"
        ) FG
        LEFT JOIN {db}.OINM N ON FG."FGCode"=N."ItemCode"
        GROUP BY FG."FGCode",FG."FGName",FG."SubGroup",FG."QtyPerUnit"
        ORDER BY "ClosingQty" DESC""")
        combined.extend(r)
    agg={}
    for row in combined:
        fg=row["FGCode"]
        if fg not in agg:
            agg[fg]={**row,"OpeningQty":0,"ProductionQty":0,"ProdSpecQty":0,"GRQty":0,"ARInvoiceQty":0,"ARSpecQty":0,"ClosingQty":0,"ClosingValue":0}
        for k in ["OpeningQty","ProductionQty","ProdSpecQty","GRQty","ARInvoiceQty","ARSpecQty","ClosingQty","ClosingValue"]:
            agg[fg][k]+=float(row.get(k) or 0)
    items=[{**v,**{k:round(v[k],0) for k in ["OpeningQty","ProductionQty","ProdSpecQty","GRQty","ARInvoiceQty","ARSpecQty","ClosingQty","ClosingValue"]}} for v in agg.values()]
    items.sort(key=lambda x:-(x.get("ClosingQty") or 0))
    # Add per-FG warehouse breakdown
    if items:
        fg_in="','".join([safe(i["FGCode"]) for i in items])
        whs_map={}
        for db in ALL_DBS:
            whs=q(f"""
            SELECT W."ItemCode" AS "FGCode",W."WhsCode",WH."WhsName",ROUND(W."OnHand",0) AS "OnHand"
            FROM {db}.OITW W JOIN {db}.OWHS WH ON W."WhsCode"=WH."WhsCode"
            WHERE W."ItemCode" IN ('{fg_in}') AND W."OnHand"!=0
            ORDER BY W."OnHand" DESC""")
            for row in whs:
                fg=row["FGCode"];key=row["WhsCode"]
                if fg not in whs_map: whs_map[fg]={}
                if key not in whs_map[fg]: whs_map[fg][key]={"WhsCode":key,"WhsName":row["WhsName"],"OnHand":0.0}
                whs_map[fg][key]["OnHand"]+=float(row.get("OnHand") or 0)
        for item in items:
            fg=item["FGCode"]
            item["Warehouses"]=sorted([{"WhsCode":w["WhsCode"],"WhsName":w["WhsName"],"OnHand":round(w["OnHand"],0)} for w in whs_map.get(fg,{}).values() if w["OnHand"]!=0],key=lambda x:-(x["OnHand"] or 0))
    totals={"OpeningQty":0,"ProductionQty":0,"ProdSpecQty":0,"GRQty":0,"ARInvoiceQty":0,"ARSpecQty":0,"ClosingQty":0,"ClosingValue":0}
    for row in items:
        for k in totals: totals[k]+=float(row.get(k) or 0)
    for k in totals: totals[k]=round(totals[k],0)
    return JSONResponse(content={"totals":totals,"items":items})

# ════════════ PLANNING ════════════
@app.get("/api/planning")
def planning(subgroup: str = Query(None)):
    sg = f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    combined = []
    for db in ALL_DBS:
        combined.extend(q(f"""WITH CONSUMPTION AS (
        SELECT N."ItemCode",
            SUM(CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-30) THEN N."OutQty" ELSE 0 END) AS "Out30d",
            SUM(CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-60) AND N."DocDate"<ADD_DAYS(CURRENT_DATE,-30) THEN N."OutQty" ELSE 0 END) AS "Out30_60d",
            SUM(CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-90) THEN N."OutQty" ELSE 0 END) AS "Out90d",
            MAX(CASE WHEN N."OutQty">0 THEN N."DocDate" END) AS "LastMoveDate"
        FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
        WHERE N."OutQty">0 AND N."DocDate">=ADD_DAYS(CURRENT_DATE,-90) AND I."U_Unit"='{UNIT}'
        GROUP BY N."ItemCode")
        SELECT M."ItemCode",M."ItemName",COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
        ROUND(SUM(W."OnHand"),0) AS "TotalOnHand",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
        ROUND(COALESCE(C."Out30d",0),0) AS "Out30d",ROUND(COALESCE(C."Out30_60d",0),0) AS "Out30_60d",
        ROUND(COALESCE(C."Out90d",0)/90,1) AS "AvgDailyOut",ROUND(COALESCE(C."Out90d",0)/3,0) AS "AvgMonthlyOut",
        CASE WHEN COALESCE(C."Out90d",0)=0 THEN -1 ELSE ROUND(SUM(W."OnHand")/(COALESCE(C."Out90d",0)/90),0) END AS "DaysOfStockLeft",
        ROUND(CASE WHEN COALESCE(C."Out90d",0)=0 THEN 0 ELSE GREATEST(0,(COALESCE(C."Out90d",0)/90)*30-SUM(W."OnHand")) END,0) AS "SuggestedOrder",
        CASE WHEN COALESCE(C."Out30d",0)=0 AND COALESCE(C."Out30_60d",0)=0 THEN 'FLAT'
             WHEN COALESCE(C."Out30d",0)>COALESCE(C."Out30_60d",0)*1.1 THEN 'RISING'
             WHEN COALESCE(C."Out30d",0)<COALESCE(C."Out30_60d",0)*0.9 THEN 'FALLING'
             ELSE 'STABLE' END AS "Trend",
        TO_DATE(C."LastMoveDate") AS "LastMoveDate",TO_DATE(M."CreateDate") AS "CreateDate"
        FROM {db}.OITM M JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN {db}.OITW W ON M."ItemCode"=W."ItemCode"
        LEFT JOIN CONSUMPTION C ON M."ItemCode"=C."ItemCode"
        WHERE G."ItmsGrpNam"='FINISHED' AND M."InvntItem"='Y' AND M."U_Unit"='{UNIT}' AND W."OnHand">0 {sg}
        GROUP BY M."ItemCode",M."ItemName",M."U_Sub_Group",C."Out30d",C."Out30_60d",C."Out90d",C."LastMoveDate",M."CreateDate"
        ORDER BY "DaysOfStockLeft" ASC"""))
    combined.sort(key=lambda x: (x.get("DaysOfStockLeft") if (x.get("DaysOfStockLeft") or -1) >= 0 else 99999))
    return JSONResponse(content={"data": combined})

# ════════════ DEBUG — check what exists in BEVERAGES DB ════════════
@app.get("/api/debug/rm_pm")
def debug_rm_pm():
    """Check actual ItmsGrpNam and U_Unit values for RM/PM items in BEVERAGES DB"""
    results = {}
    # 1. What group names exist?
    grp = q(f"""SELECT DISTINCT G."ItmsGrpNam", COUNT(*) AS "Items"
        FROM {DB3}.OITM M JOIN {DB3}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE M."InvntItem"='Y'
        GROUP BY G."ItmsGrpNam" ORDER BY "Items" DESC""")
    results['all_groups_in_BEV_DB'] = grp

    # 2. What U_Unit values exist for RM?
    units = q(f"""SELECT DISTINCT M."U_Unit", G."ItmsGrpNam", COUNT(*) AS "Items"
        FROM {DB3}.OITM M JOIN {DB3}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE M."InvntItem"='Y'
        AND G."ItmsGrpNam" IN ('RAW MATERIAL','PACKAGING MATERIAL')
        GROUP BY M."U_Unit", G."ItmsGrpNam" ORDER BY "Items" DESC""")
    results['rm_pm_unit_values'] = units

    # 3. Stock with no unit filter
    stock = q(f"""SELECT G."ItmsGrpNam", COUNT(DISTINCT W."ItemCode") AS "SKUs",
        ROUND(SUM(W."OnHand"),0) AS "Qty"
        FROM {DB3}.OITW W JOIN {DB3}.OITM M ON W."ItemCode"=M."ItemCode"
        JOIN {DB3}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE M."InvntItem"='Y'
        AND G."ItmsGrpNam" IN ('RAW MATERIAL','PACKAGING MATERIAL')
        AND W."OnHand">0
        GROUP BY G."ItmsGrpNam" """)
    results['rm_pm_stock_no_unit_filter'] = stock

    # 4. With BEVERAGES filter
    stock_bev = q(f"""SELECT G."ItmsGrpNam", COUNT(DISTINCT W."ItemCode") AS "SKUs",
        ROUND(SUM(W."OnHand"),0) AS "Qty"
        FROM {DB3}.OITW W JOIN {DB3}.OITM M ON W."ItemCode"=M."ItemCode"
        JOIN {DB3}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='BEVERAGES'
        AND G."ItmsGrpNam" IN ('RAW MATERIAL','PACKAGING MATERIAL')
        AND W."OnHand">0
        GROUP BY G."ItmsGrpNam" """)
    results['rm_pm_stock_with_BEVERAGES_filter'] = stock_bev

    return JSONResponse(content={"data": results})


# ════════════ CHAT ════════════
@app.post("/api/chat")
async def chat(request: Request):
    if not GROQ_API_KEY: return JSONResponse(content={"reply": "Set GROQ_API_KEY."})
    try:
        body = await request.json(); msg = body.get("message", ""); ctx = body.get("context", "")
        import httpx
        async with httpx.AsyncClient(timeout=60) as cl:
            r = await cl.post("https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": "llama-3.3-70b-versatile", "max_tokens": 1024,
                      "messages": [{"role": "system", "content": f"Inventory analyst for Jivo Beverages. Context:\n{ctx}\nBe concise. Indian format (Cr/L)."},
                                   {"role": "user", "content": msg}]})
            d = r.json(); ch = d.get("choices", [])
            return JSONResponse(content={"reply": ch[0]["message"]["content"] if ch else d.get("error", {}).get("message", "Error")})
    except Exception as e:
        return JSONResponse(content={"reply": str(e)})

@app.get("/", response_class=HTMLResponse)
async def serve():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "dashboard_beverages.html"), "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/conveyor", response_class=HTMLResponse)
async def conveyor():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "conveyor_sample.html"), "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8006)
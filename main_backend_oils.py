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

SCHEMAS = {"jivo_oil": "JIVO_OIL_HANADB", "jivo_mart": "JIVO_MART_HANADB"}
def get_schema(s): return SCHEMAS.get(s, "JIVO_OIL_HANADB")
def conn(): return dbapi.connect(address='103.89.45.192', port=30015, user='DATA1', password='Jivo@1989')

def cv(v):
    if v is None: return None
    try:
        if pd.isna(v): return None
    except: pass
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
    if isinstance(v, pd.Timestamp): return v.strftime("%Y-%m-%d")
    if hasattr(v, 'item'): return v.item()
    if isinstance(v, (int, float)): return v
    return str(v) if not isinstance(v, str) else v

def q(sql):
    c = None
    try:
        c = conn()
        df = pd.read_sql(sql, c)
        for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64']).columns:
            df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else None)
        return [{k: cv(v) for k, v in r.items()} for r in df.to_dict(orient="records")]
    except:
        traceback.print_exc()
        return []
    finally:
        if c:
            try: c.close()
            except: pass

# GIFT PACK removed from both lists
FG_VALID = ("'OLIVE','CANOLA','MUSTARD','SEEDS','SOYABEAN','SUNFLOWER',"
            "'GHEE','BLENDED','GROUNDNUT','SPICES','RICE BRAN','HONEY','COCONUT',"
            "'FLAKES','VITAMINS','COFFEE','DRY FRUITS/NUTS','SESAME','COTTON SEED',"
            "'SLICED OLIVE','RICE','ATTA','SOYA CHUNK','TEA','DRINKS','SNACKS','PALMOLEIN',"
            "'YELLOW MUSTARD'")
PM_VALID = ("'LABEL','CARTON','TIKKI','CAPS','PET BOTTLES','POUCH','TIN','SHRINK',"
            "'HDPE BOTTLES','GLASS BOTTLES','PET JAR','TAPE','PREFORM',"
            "'THERMOCOL','POLYBAG','DRUM','STEEL JAR','COUPON'")
RM_VALID = ("'SPICES','OLIVE','CANOLA','DRY FRUITS/NUTS','SEEDS','VITAMINS','MUSTARD',"
            "'SUNFLOWER','COCONUT','SOYABEAN','BLENDED','SESAME','GHEE','COFFEE',"
            "'RICE BRAN','GROUNDNUT','VEGETABLE OIL','PALMOLEIN','RICE','COTTON SEED',"
            "'MAIZE FLOUR (MAKKA ATTA)','YELLOW MUSTARD'")

GIFT_EXCL = "AND M.\"U_Sub_Group\" NOT IN ('GIFT PACK')"

def cf(c):
    if c and c.upper() in ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL'):
        return f"AND G.\"ItmsGrpNam\"='{c.upper()}'"
    return "AND G.\"ItmsGrpNam\" IN ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL')"

def tf(t): return f"AND M.\"U_TYPE\"='{t}'" if t and t != 'all' else ""
def wf(w): return f"AND W.\"WhsCode\"='{w.replace(chr(39),chr(39)+chr(39))}'" if w else ""
def whs_f(whs):
    if not whs: return ""
    codes=["'"+safe(c.strip())+"'" for c in whs.split(',') if c.strip()]
    return f"AND W.\"WhsCode\" IN ({','.join(codes)})" if codes else ""
def safe(s): return s.replace("'","''") if s else ""

# Owner join helper — handles int/varchar type mismatch
OWN_JOIN = 'LEFT JOIN {db}.OUSR U ON CAST({tbl}."U_Owner" AS VARCHAR(20))=CAST(U."USERID" AS VARCHAR(20))'

@app.get("/api/kpi")
def kpi(category:str=Query(None),schema:str=Query("jivo_oil"),whs:str=Query(None)):
    db=get_schema(schema);f=cf(category);wf_=wf(whs)
    return JSONResponse(content={"data":q(f"""SELECT
    ROUND((SELECT SUM(W."OnHand") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {wf_} {GIFT_EXCL.replace('M.','M.')}),0) AS "TotalQty",
    ROUND((SELECT SUM(W."OnHand"*M."LastPurPrc") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}),0) AS "TotalValue",
    (SELECT COUNT(DISTINCT W."ItemCode") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}) AS "TotalSKUs",
    (SELECT COUNT(*) FROM (SELECT M."ItemCode" FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} {wf_} {GIFT_EXCL} GROUP BY M."ItemCode" HAVING SUM(W."OnHand")<=0)) AS "OutOfStockSKUs"
    FROM DUMMY""")})

@app.get("/api/categories")
def categories(schema:str=Query("jivo_oil")):
    db=get_schema(schema)
    return JSONResponse(content={"data":q(f"""SELECT G."ItmsGrpNam" AS "Category",COUNT(DISTINCT W."ItemCode") AS "SKUs",ROUND(SUM(W."OnHand"),0) AS "Qty",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "Value"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' AND G."ItmsGrpNam" IN ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL') AND W."OnHand">0 {GIFT_EXCL}
    GROUP BY G."ItmsGrpNam" ORDER BY "Value" DESC""")})

@app.get("/api/out_of_stock")
def out_of_stock(category:str=Query(None),schema:str=Query("jivo_oil")):
    db=get_schema(schema);f=cf(category)
    return JSONResponse(content={"data":q(f"""SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",ROUND(SUM(W."OnHand"),0) AS "TotalOnHand"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} {GIFT_EXCL}
    GROUP BY G."ItmsGrpNam",M."ItemCode",M."ItemName" HAVING SUM(W."OnHand")<=0 ORDER BY G."ItmsGrpNam",M."ItemName" """)})

@app.get("/api/warehouses")
def warehouses(schema:str=Query("jivo_oil")):
    db=get_schema(schema)
    return JSONResponse(content={"data":q(f"""
    SELECT W."WhsCode",W."WhsName",COALESCE(U."U_NAME",'–') AS "OwnerName"
    FROM {db}.OWHS W
    LEFT JOIN {db}.OUSR U ON CAST(W."U_Owner" AS VARCHAR(20))=CAST(U."USERID" AS VARCHAR(20))
    WHERE W."WhsCode" NOT IN ('01','BH-FA','DL-FA','GP-FA','MY-FA','DL','HR')
    ORDER BY W."WhsName" """)})

@app.get("/api/warehouse_summary")
def warehouse_summary(category:str=Query(None),schema:str=Query("jivo_oil"),owner:str=Query(None)):
    db=get_schema(schema);f=cf(category)
    owner_f=f"AND COALESCE(U.\"U_NAME\",'–')='{safe(owner)}'" if owner else ""
    return JSONResponse(content={"data":q(f"""
    SELECT W."WhsCode",H."WhsName",COALESCE(U."U_NAME",'–') AS "OwnerName",
        COUNT(DISTINCT W."ItemCode") AS "SKUs",
        ROUND(SUM(W."OnHand"),0) AS "Qty",ROUND(SUM(W."OnOrder"),0) AS "OnOrder",
        ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "Value"
    FROM {db}.OITW W
    JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
    JOIN {db}.OWHS H ON W."WhsCode"=H."WhsCode"
    LEFT JOIN {db}.OUSR U ON CAST(H."U_Owner" AS VARCHAR(20))=CAST(U."USERID" AS VARCHAR(20))
    JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {GIFT_EXCL} {owner_f}
    GROUP BY W."WhsCode",H."WhsName",U."U_NAME" ORDER BY "Value" DESC""")})

@app.get("/api/warehouse_items")
def warehouse_items(whs:str=Query(""),category:str=Query(None),schema:str=Query("jivo_oil")):
    db=get_schema(schema);f=cf(category);s=safe(whs)
    return JSONResponse(content={"data":q(f"""
    SELECT G."ItmsGrpNam" AS "Category",W."ItemCode",M."ItemName",
        COALESCE(M."U_Sub_Group",'–') AS "SubGroup",COALESCE(M."U_TYPE",'–') AS "ItemType",
        ROUND(W."OnHand",0) AS "OnHand",ROUND(W."OnOrder",0) AS "OnOrder",
        ROUND(W."OnHand"-W."IsCommited"+W."OnOrder",0) AS "Available",
        ROUND(W."OnHand"*M."LastPurPrc",0) AS "StockValue"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."WhsCode"='{s}' AND W."OnHand">0 {GIFT_EXCL}
    ORDER BY "StockValue" DESC""")})

@app.get("/api/warehouse_owners")
def warehouse_owners(schema:str=Query("jivo_oil")):
    db=get_schema(schema)
    return JSONResponse(content={"data":q(f"""
    SELECT DISTINCT COALESCE(U."U_NAME",'–') AS "OwnerName"
    FROM {db}.OWHS H
    LEFT JOIN {db}.OUSR U ON CAST(H."U_Owner" AS VARCHAR(20))=CAST(U."USERID" AS VARCHAR(20))
    WHERE H."WhsCode" NOT IN ('01','BH-FA','DL-FA','GP-FA','MY-FA','DL','HR')
    ORDER BY "OwnerName" """)})

@app.get("/api/stock_position")
def stock_position(category:str=Query(None),schema:str=Query("jivo_oil"),whs:str=Query(None)):
    db=get_schema(schema);f=cf(category);wf_=wf(whs)
    return JSONResponse(content={"data":q(f"""SELECT G."ItmsGrpNam" AS "Category",W."ItemCode",M."ItemName",
    W."WhsCode",H."WhsName",COALESCE(U."U_NAME",'–') AS "OwnerName",
    ROUND(W."OnHand",0) AS "OnHand",ROUND(W."OnOrder",0) AS "OnOrder",
    ROUND(W."OnHand"-W."IsCommited"+W."OnOrder",0) AS "Available",ROUND(W."OnHand"*M."LastPurPrc",0) AS "StockValue"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
    JOIN {db}.OWHS H ON W."WhsCode"=H."WhsCode"
    LEFT JOIN {db}.OUSR U ON CAST(H."U_Owner" AS VARCHAR(20))=CAST(U."USERID" AS VARCHAR(20))
    JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}
    ORDER BY "StockValue" DESC""")})

@app.get("/api/movement")
def movement(days:int=Query(30),category:str=Query(None),schema:str=Query("jivo_oil"),
             date_from:str=Query(None),date_to:str=Query(None),whs:str=Query(None)):
    if days not in (7,15,30,60,90): days=30
    db=get_schema(schema);f=cf(category)
    date_filter=f"AND N.\"DocDate\">='{date_from}' AND N.\"DocDate\"<='{date_to}'" if date_from and date_to else f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    whs_f=f"AND N.\"Warehouse\"='{safe(whs)}'" if whs else ""
    return JSONResponse(content={"data":q(f"""
    SELECT TO_DATE(N."DocDate") AS "Date",N."Warehouse" AS "WhsCode",
        COALESCE(H."WhsName",N."Warehouse") AS "WhsName",
        M."ItemCode",M."ItemName",G."ItmsGrpNam" AS "Category",
        COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
        ROUND(SUM(N."InQty"),0) AS "InQty",ROUND(SUM(N."OutQty"),0) AS "OutQty",
        ROUND(SUM(N."InQty"*N."Price"),0) AS "InValue",ROUND(SUM(N."OutQty"*N."Price"),0) AS "OutValue"
    FROM {db}.OINM N JOIN {db}.OITM M ON N."ItemCode"=M."ItemCode"
    JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN {db}.OWHS H ON N."Warehouse"=H."WhsCode"
    WHERE M."U_Unit"='OIL' {f} {GIFT_EXCL} {date_filter} {whs_f}
    GROUP BY TO_DATE(N."DocDate"),N."Warehouse",H."WhsName",M."ItemCode",M."ItemName",G."ItmsGrpNam",M."U_Sub_Group"
    ORDER BY TO_DATE(N."DocDate") DESC,"OutValue" DESC""")})

@app.get("/api/movers_summary")
def movers_summary(days:int=Query(30),category:str=Query(None),schema:str=Query("jivo_oil"),
                   date_from:str=Query(None),date_to:str=Query(None),whs:str=Query(None)):
    db=get_schema(schema);f=cf(category);wf2=whs_f(whs)
    date_filter=f"AND N.\"DocDate\">='{date_from}' AND N.\"DocDate\"<='{date_to}'" if date_from and date_to else f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    return JSONResponse(content={"data":q(f"""SELECT X."MovementStatus" AS "Status",COUNT(*) AS "Count",ROUND(SUM(X."StockValue"),0) AS "Value",ROUND(SUM(X."TotalOnHand"),0) AS "Qty"
    FROM (SELECT M."ItemCode",SUM(W."OnHand") AS "TotalOnHand",SUM(W."OnHand"*M."LastPurPrc") AS "StockValue",
        CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN 'NON-MOVING' WHEN MV."TotalOut"<50 THEN 'SLOW' WHEN MV."TotalOut"<500 THEN 'MEDIUM' ELSE 'FAST' END AS "MovementStatus"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN (SELECT N."ItemCode",SUM(N."OutQty") AS "TotalOut" FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
        WHERE N."OutQty">0 {date_filter} AND I."U_Unit"='OIL' GROUP BY N."ItemCode") MV ON M."ItemCode"=MV."ItemCode"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {wf2} {GIFT_EXCL}
    GROUP BY M."ItemCode",MV."TotalOut") X
    GROUP BY X."MovementStatus" ORDER BY CASE X."MovementStatus" WHEN 'NON-MOVING' THEN 1 WHEN 'SLOW' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END""")})

@app.get("/api/movers_by_subgroup")
def movers_by_subgroup(days:int=Query(30),item_type:str=Query(None),category:str=Query("FINISHED"),schema:str=Query("jivo_oil"),
                       date_from:str=Query(None),date_to:str=Query(None),whs:str=Query(None)):
    db=get_schema(schema);type_f=tf(item_type);wf2=whs_f(whs)
    cat_f=f"AND G.\"ItmsGrpNam\"='{category.upper()}'"
    valid=FG_VALID if category=='FINISHED' else (PM_VALID if category=='PACKAGING MATERIAL' else RM_VALID)
    date_filter=f"AND N.\"DocDate\">='{date_from}' AND N.\"DocDate\"<='{date_to}'" if date_from and date_to else f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    return JSONResponse(content={"data":q(f"""
    SELECT COALESCE(CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE M."U_Sub_Group" END,'UNCLASSIFIED') AS "SubGroup",
        COUNT(DISTINCT M."ItemCode") AS "TotalSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN 1 ELSE 0 END) AS "NonMovingSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>0 AND COALESCE(MV."TotalOut",0)<50 THEN 1 ELSE 0 END) AS "SlowSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>=50 AND COALESCE(MV."TotalOut",0)<500 THEN 1 ELSE 0 END) AS "MediumSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>=500 THEN 1 ELSE 0 END) AS "FastSKUs",
        ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
        ROUND(SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN W."OnHand"*M."LastPurPrc" ELSE 0 END),0) AS "StuckValue"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN (SELECT N."ItemCode",SUM(N."OutQty") AS "TotalOut" FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
        WHERE N."OutQty">0 {date_filter} AND I."U_Unit"='OIL' GROUP BY N."ItemCode") MV ON M."ItemCode"=MV."ItemCode"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {cat_f} AND W."OnHand">0 {wf2} AND M."U_Sub_Group" IN ({valid}) {type_f}
    GROUP BY CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE M."U_Sub_Group" END
    ORDER BY "StuckValue" DESC,"StockValue" DESC""")})

@app.get("/api/movers")
def movers(days:int=Query(30),category:str=Query(None),subgroup:str=Query(None),item_type:str=Query(None),schema:str=Query("jivo_oil"),
           date_from:str=Query(None),date_to:str=Query(None),whs:str=Query(None)):
    db=get_schema(schema);f=cf(category);wf2=whs_f(whs)
    sg="AND M.\"U_Sub_Group\"='MUSTARD' AND M.\"U_TYPE\"='PREMIUM'" if subgroup=='YELLOW MUSTARD' else (f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else "")
    type_f=tf(item_type)
    if date_from and date_to:
        date_filter=f"AND N.\"DocDate\">='{date_from}' AND N.\"DocDate\"<='{date_to}'"
        day_div=f"(DAYS_BETWEEN(TO_DATE('{date_from}'),TO_DATE('{date_to}'))+1)"
    else:
        date_filter=f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
        day_div=str(days)
    return JSONResponse(content={"data":q(f"""
    SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",
        CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE COALESCE(M."U_Sub_Group",'–') END AS "SubGroup",
        COALESCE(M."U_TYPE",'–') AS "ItemType",
        ROUND(SUM(W."OnHand"),0) AS "TotalOnHand",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
        COALESCE(MV."TotalOut",0) AS "Out{days}d",TO_DATE(MV."LastMoveDate") AS "LastMoveDate",
        CASE WHEN MV."LastMoveDate" IS NULL THEN -1 ELSE DAYS_BETWEEN(MV."LastMoveDate",CURRENT_DATE) END AS "DaysSinceMove",
        CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN 'NON-MOVING' WHEN MV."TotalOut"<50 THEN 'SLOW' WHEN MV."TotalOut"<500 THEN 'MEDIUM' ELSE 'FAST' END AS "MovementStatus",
        CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN -1 ELSE ROUND(SUM(W."OnHand")/(MV."TotalOut"/{day_div}),0) END AS "DaysOfStockLeft",
        STRING_AGG(W."WhsCode", ', ') AS "WhsCodes"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN (SELECT N."ItemCode",SUM(N."OutQty") AS "TotalOut",MAX(CASE WHEN N."OutQty">0 THEN N."DocDate" END) AS "LastMoveDate"
        FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
        WHERE N."OutQty">0 {date_filter} AND I."U_Unit"='OIL' GROUP BY N."ItemCode") MV ON M."ItemCode"=MV."ItemCode"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {wf2} {sg} {type_f} {GIFT_EXCL}
    GROUP BY G."ItmsGrpNam",M."ItemCode",M."ItemName",M."U_Sub_Group",M."U_TYPE",MV."TotalOut",MV."LastMoveDate"
    ORDER BY COALESCE(MV."TotalOut",0) ASC,"StockValue" DESC""")})

@app.get("/api/not_billed_summary")
def not_billed_summary(schema:str=Query("jivo_oil")):
    db=get_schema(schema)
    parts=[]
    for d in [30,60,90]:
        parts.append(f"""SELECT '{d} Days' AS "Period",
        COUNT(DISTINCT CASE WHEN B."ItemCode" IS NULL THEN M."ItemCode" END) AS "NotBilledSKUs",
        ROUND(SUM(CASE WHEN B."ItemCode" IS NULL THEN W."OnHand"*M."LastPurPrc" ELSE 0 END),0) AS "NotBilledValue"
        FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
        LEFT JOIN (SELECT DISTINCT L."ItemCode" FROM {db}.OINV I JOIN {db}.INV1 L ON I."DocEntry"=L."DocEntry"
            WHERE I."CANCELED"='N' AND I."DocDate">=ADD_DAYS(CURRENT_DATE,-{d})) B ON M."ItemCode"=B."ItemCode"
        WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' AND G."ItmsGrpNam"='FINISHED'
          AND W."OnHand">0 AND M."CreateDate"<ADD_DAYS(CURRENT_DATE,-30) {GIFT_EXCL}""")
    return JSONResponse(content={"data":q(" UNION ALL ".join(parts))})

@app.get("/api/not_billed_by_subgroup")
def not_billed_by_subgroup(days:int=Query(30),item_type:str=Query(None),schema:str=Query("jivo_oil"),
                           date_from:str=Query(None),date_to:str=Query(None),whs:str=Query(None)):
    db=get_schema(schema);type_f=tf(item_type);wf2=whs_f(whs)
    bill_filter=f"AND I.\"DocDate\">='{date_from}' AND I.\"DocDate\"<='{date_to}'" if date_from and date_to else f"AND I.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    return JSONResponse(content={"data":q(f"""
    SELECT COALESCE(M."U_Sub_Group",'UNCLASSIFIED') AS "SubGroup",
        COUNT(DISTINCT M."ItemCode") AS "TotalSKUs",
        COUNT(DISTINCT CASE WHEN B."ItemCode" IS NULL THEN M."ItemCode" END) AS "NotBilledSKUs",
        ROUND(SUM(CASE WHEN B."ItemCode" IS NULL THEN W."OnHand"*M."LastPurPrc" ELSE 0 END),0) AS "NotBilledValue",
        ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "TotalValue"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN (SELECT DISTINCT L."ItemCode" FROM {db}.OINV I JOIN {db}.INV1 L ON I."DocEntry"=L."DocEntry"
        WHERE I."CANCELED"='N' {bill_filter}) B ON M."ItemCode"=B."ItemCode"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' AND G."ItmsGrpNam"='FINISHED'
      AND W."OnHand">0 AND M."CreateDate"<ADD_DAYS(CURRENT_DATE,-30) AND M."U_Sub_Group" IN ({FG_VALID}) {type_f} {wf2}
    GROUP BY M."U_Sub_Group" ORDER BY "NotBilledValue" DESC""")})

@app.get("/api/not_billed")
def not_billed(days:int=Query(30),subgroup:str=Query(None),item_type:str=Query(None),schema:str=Query("jivo_oil"),
               date_from:str=Query(None),date_to:str=Query(None),whs:str=Query(None)):
    db=get_schema(schema)
    sg=f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    type_f=tf(item_type);wf2=whs_f(whs)
    rc_filter=f"AND I.\"DocDate\">='{date_from}' AND I.\"DocDate\"<='{date_to}'" if date_from and date_to else f"AND I.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
    return JSONResponse(content={"data":q(f"""
    SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",
        COALESCE(M."U_Sub_Group",'–') AS "SubGroup",COALESCE(M."U_TYPE",'–') AS "ItemType",
        ROUND(SUM(W."OnHand"),0) AS "CurrentStock",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
        TO_DATE(LB."LastBillDate") AS "LastBillDate",
        CASE WHEN LB."LastBillDate" IS NULL THEN 'NEVER BILLED'
             ELSE CAST(DAYS_BETWEEN(LB."LastBillDate",CURRENT_DATE) AS VARCHAR)||' days ago' END AS "LastBilledAgo",
        LB."LastCustomer",TO_DATE(M."CreateDate") AS "CreatedOn",
        STRING_AGG(W."WhsCode", ', ') AS "WhsCodes"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN (SELECT L."ItemCode",MAX(I."DocDate") AS "LastBillDate",MAX(I."CardName") AS "LastCustomer"
        FROM {db}.OINV I JOIN {db}.INV1 L ON I."DocEntry"=L."DocEntry" WHERE I."CANCELED"='N' GROUP BY L."ItemCode") LB ON M."ItemCode"=LB."ItemCode"
    LEFT JOIN (SELECT DISTINCT L."ItemCode" FROM {db}.OINV I JOIN {db}.INV1 L ON I."DocEntry"=L."DocEntry"
        WHERE I."CANCELED"='N' {rc_filter}) RC ON M."ItemCode"=RC."ItemCode"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' AND G."ItmsGrpNam"='FINISHED'
      AND W."OnHand">0 AND RC."ItemCode" IS NULL AND M."CreateDate"<ADD_DAYS(CURRENT_DATE,-30) {GIFT_EXCL} {sg} {type_f} {wf2}
    GROUP BY G."ItmsGrpNam",M."ItemCode",M."ItemName",M."U_Sub_Group",M."U_TYPE",LB."LastBillDate",LB."LastCustomer",M."CreateDate"
    ORDER BY "StockValue" DESC""")})

def abc_inner(db):
    return f"""SELECT M."ItemCode",M."ItemName",COALESCE(M."U_Sub_Group",'UNCLASSIFIED') AS "SubGroup",COALESCE(M."U_TYPE",'–') AS "ItemType",
    ROUND(SUM(W."OnHand"),0) AS "TotalOnHand",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
    ROW_NUMBER() OVER (ORDER BY SUM(W."OnHand"*M."LastPurPrc") DESC) AS "Rank",
    ROUND(SUM(SUM(W."OnHand"*M."LastPurPrc")) OVER (ORDER BY SUM(W."OnHand"*M."LastPurPrc") DESC)/NULLIF(SUM(SUM(W."OnHand"*M."LastPurPrc")) OVER(),0)*100,2) AS "CumulativePct"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' AND G."ItmsGrpNam"='FINISHED' AND W."OnHand">0 AND M."U_Sub_Group" NOT IN ('GIFT PACK')
    GROUP BY M."ItemCode",M."ItemName",M."U_Sub_Group",M."U_TYPE" """

def xyz_cte(db):
    return f"""MONTHLY AS (SELECT N."ItemCode",
        CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-30) THEN 'M1' WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-60) THEN 'M2' ELSE 'M3' END AS "Month",
        SUM(N."OutQty") AS "MonthlyOut"
        FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode" JOIN {db}.OITB G ON I."ItmsGrpCod"=G."ItmsGrpCod"
        WHERE N."OutQty">0 AND N."DocDate">=ADD_DAYS(CURRENT_DATE,-90) AND I."U_Unit"='OIL' AND G."ItmsGrpNam"='FINISHED'
          AND I."U_Sub_Group" NOT IN ('GIFT PACK')
        GROUP BY N."ItemCode",CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-30) THEN 'M1' WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-60) THEN 'M2' ELSE 'M3' END),
    STATS AS (SELECT "ItemCode",AVG("MonthlyOut") AS "AvgOut",STDDEV("MonthlyOut") AS "StdOut" FROM MONTHLY GROUP BY "ItemCode"),
    XYZ_BASE AS (SELECT S."ItemCode",ROUND(S."AvgOut",1) AS "AvgMonthlyOut",
        CASE WHEN S."AvgOut">0 THEN ROUND(S."StdOut"/S."AvgOut",4) ELSE 9999 END AS "CoV",
        CASE WHEN S."AvgOut" IS NULL OR S."AvgOut"=0 THEN 'Z' WHEN S."StdOut"/S."AvgOut"<0.5 THEN 'X' WHEN S."StdOut"/S."AvgOut"<1.0 THEN 'Y' ELSE 'Z' END AS "XYZClass"
        FROM STATS S)"""

@app.get("/api/abcxyz_summary")
def abcxyz_summary(schema:str=Query("jivo_oil")):
    db=get_schema(schema);AI=abc_inner(db);XC=xyz_cte(db)
    return JSONResponse(content={"data":q(f"""WITH ABC_BASE AS (SELECT "ItemCode","StockValue",CASE WHEN "CumulativePct"<=80 THEN 'A' WHEN "CumulativePct"<=95 THEN 'B' ELSE 'C' END AS "ABCClass" FROM ({AI}) X),
    {XC},COMBINED AS (SELECT A."ABCClass",COALESCE(X."XYZClass",'Z') AS "XYZClass",A."ABCClass"||COALESCE(X."XYZClass",'Z') AS "Combo",A."StockValue" FROM ABC_BASE A LEFT JOIN XYZ_BASE X ON A."ItemCode"=X."ItemCode")
    SELECT "Combo" AS "ABCXYZClass","ABCClass","XYZClass",COUNT(*) AS "SKUs",ROUND(SUM("StockValue"),0) AS "Value" FROM COMBINED GROUP BY "Combo","ABCClass","XYZClass" ORDER BY "ABCClass","XYZClass" """)})

@app.get("/api/abcxyz_by_subgroup")
def abcxyz_by_subgroup(item_type:str=Query(None),schema:str=Query("jivo_oil")):
    db=get_schema(schema);type_f=tf(item_type);AI=abc_inner(db);XC=xyz_cte(db)
    return JSONResponse(content={"data":q(f"""WITH ABC_BASE AS (SELECT "ItemCode","ItemName","SubGroup","ItemType","TotalOnHand","StockValue","CumulativePct","Rank",
        CASE WHEN "CumulativePct"<=80 THEN 'A' WHEN "CumulativePct"<=95 THEN 'B' ELSE 'C' END AS "ABCClass" FROM ({AI}) X),
    {XC},COMBINED AS (SELECT A."SubGroup",COUNT(*) AS "TotalSKUs",ROUND(SUM(A."StockValue"),0) AS "StockValue",
        SUM(CASE WHEN A."ABCClass"='A' THEN 1 ELSE 0 END) AS "A_Count",SUM(CASE WHEN A."ABCClass"='B' THEN 1 ELSE 0 END) AS "B_Count",SUM(CASE WHEN A."ABCClass"='C' THEN 1 ELSE 0 END) AS "C_Count",
        SUM(CASE WHEN COALESCE(X."XYZClass",'Z')='X' THEN 1 ELSE 0 END) AS "X_Count",SUM(CASE WHEN COALESCE(X."XYZClass",'Z')='Y' THEN 1 ELSE 0 END) AS "Y_Count",SUM(CASE WHEN COALESCE(X."XYZClass",'Z')='Z' THEN 1 ELSE 0 END) AS "Z_Count"
        FROM ABC_BASE A LEFT JOIN XYZ_BASE X ON A."ItemCode"=X."ItemCode"
        JOIN {db}.OITM M ON A."ItemCode"=M."ItemCode"
        WHERE A."SubGroup" IN ({FG_VALID}) {type_f} GROUP BY A."SubGroup")
    SELECT * FROM COMBINED ORDER BY "StockValue" DESC""")})

@app.get("/api/abcxyz")
def abcxyz(subgroup:str=Query(None),item_type:str=Query(None),combo:str=Query(None),schema:str=Query("jivo_oil")):
    db=get_schema(schema)
    sg=f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    type_f=tf(item_type)
    combo_f=f"AND A.\"ABCClass\"||COALESCE(X.\"XYZClass\",'Z')='{safe(combo)}'" if combo and combo!='all' else ""
    AI=abc_inner(db);XC=xyz_cte(db)
    return JSONResponse(content={"data":q(f"""WITH ABC_BASE AS (SELECT "ItemCode","ItemName","SubGroup","ItemType","TotalOnHand","StockValue","CumulativePct","Rank",
        CASE WHEN "CumulativePct"<=80 THEN 'A' WHEN "CumulativePct"<=95 THEN 'B' ELSE 'C' END AS "ABCClass" FROM ({AI}) X),
    {XC}
    SELECT A."ItemCode",A."ItemName",A."SubGroup",A."ItemType",A."TotalOnHand",A."StockValue",A."CumulativePct",A."Rank",A."ABCClass",
        COALESCE(X."XYZClass",'Z') AS "XYZClass",COALESCE(X."AvgMonthlyOut",0) AS "AvgMonthlyOut",COALESCE(X."CoV",9999) AS "CoV",
        A."ABCClass"||COALESCE(X."XYZClass",'Z') AS "ABCXYZClass"
    FROM ABC_BASE A LEFT JOIN XYZ_BASE X ON A."ItemCode"=X."ItemCode"
    JOIN {db}.OITM M ON A."ItemCode"=M."ItemCode"
    WHERE 1=1 {sg} {type_f} {combo_f} ORDER BY A."Rank" """)})

@app.get("/api/aging")
def aging(category:str=Query(None),schema:str=Query("jivo_oil"),whs:str=Query(None)):
    db=get_schema(schema);f=cf(category);wf2=whs_f(whs)
    return JSONResponse(content={"data":q(f"""
    SELECT G."ItmsGrpNam" AS "Category",
        CASE WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=30 THEN '0-30'
             WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=60 THEN '31-60'
             WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=90 THEN '61-90'
             ELSE '90+' END AS "Bucket",
        COUNT(DISTINCT W."ItemCode"||'|'||W."WhsCode") AS "Items",
        ROUND(SUM(W."OnHand"),0) AS "Qty",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "Value"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    JOIN (SELECT N."ItemCode",N."Warehouse",MIN(N."DocDate") AS "FirstDate" FROM {db}.OINM N WHERE N."InQty">0 GROUP BY N."ItemCode",N."Warehouse") FR
         ON W."ItemCode"=FR."ItemCode" AND W."WhsCode"=FR."Warehouse"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {GIFT_EXCL} {wf2}
    GROUP BY G."ItmsGrpNam",CASE WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=30 THEN '0-30'
        WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=60 THEN '31-60'
        WHEN DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<=90 THEN '61-90' ELSE '90+' END
    ORDER BY "Category",MIN(DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE))""")})

@app.get("/api/aging_drill")
def aging_drill(bucket:str=Query("0-30"),category:str=Query(None),schema:str=Query("jivo_oil"),whs:str=Query(None)):
    db=get_schema(schema);f=cf(category);wf2=whs_f(whs)
    lo_hi={"0-30":(0,30),"31-60":(31,60),"61-90":(61,90),"90+":(91,99999)}
    lo,hi=lo_hi.get(bucket,(0,30))
    return JSONResponse(content={"data":q(f"""
    SELECT G."ItmsGrpNam" AS "Category",W."ItemCode",M."ItemName",W."WhsCode",
        TO_DATE(FR."FirstDate") AS "FirstReceiptDate",
        DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE) AS "DaysSitting",
        ROUND(W."OnHand",0) AS "Qty",ROUND(W."OnHand"*M."LastPurPrc",0) AS "Value"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    JOIN (SELECT N."ItemCode",N."Warehouse",MIN(N."DocDate") AS "FirstDate" FROM {db}.OINM N WHERE N."InQty">0 GROUP BY N."ItemCode",N."Warehouse") FR
         ON W."ItemCode"=FR."ItemCode" AND W."WhsCode"=FR."Warehouse"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {GIFT_EXCL} {wf2}
      AND DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)>={lo} AND DAYS_BETWEEN(FR."FirstDate",CURRENT_DATE)<={hi}
    ORDER BY "DaysSitting" DESC,"Value" DESC""")})

@app.get("/api/trace_subgroups")
def trace_subgroups(category:str=Query("FINISHED"),schema:str=Query("jivo_oil")):
    db=get_schema(schema)
    valid=FG_VALID if category=='FINISHED' else (PM_VALID if category=='PACKAGING MATERIAL' else RM_VALID)
    return JSONResponse(content={"data":q(f"""
    SELECT M."U_Sub_Group" AS "SubGroup",COUNT(DISTINCT M."ItemCode") AS "SKUs",
        ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "OnHand"
    FROM {db}.OITM M JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN {db}.OITW W ON M."ItemCode"=W."ItemCode"
    WHERE G."ItmsGrpNam"='{category.upper()}' AND M."InvntItem"='Y' AND M."U_Unit"='OIL' AND M."U_Sub_Group" IN ({valid})
    GROUP BY M."U_Sub_Group" ORDER BY SUM(W."OnHand") DESC NULLS LAST""")})

@app.get("/api/trace_items")
def trace_items(category:str=Query("FINISHED"),subgroup:str=Query(None),schema:str=Query("jivo_oil")):
    db=get_schema(schema)
    sg=f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    valid=FG_VALID if category=='FINISHED' else (PM_VALID if category=='PACKAGING MATERIAL' else RM_VALID)
    return JSONResponse(content={"data":q(f"""
    SELECT M."ItemCode",M."ItemName",COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
        COALESCE(M."U_TYPE",'–') AS "ItemType",
        ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "OnHand",ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),0) AS "StockValue"
    FROM {db}.OITM M JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN {db}.OITW W ON M."ItemCode"=W."ItemCode"
    WHERE G."ItmsGrpNam"='{category.upper()}' AND M."InvntItem"='Y' AND M."U_Unit"='OIL' AND M."U_Sub_Group" IN ({valid}) {sg}
    GROUP BY M."ItemCode",M."ItemName",M."U_Sub_Group",M."U_TYPE"
    ORDER BY SUM(W."OnHand") DESC NULLS LAST,M."ItemName" """)})

@app.get("/api/trace_header")
def trace_header(item:str=Query(""),schema:str=Query("jivo_oil")):
    db=get_schema(schema);s=safe(item)
    return JSONResponse(content={"data":q(f"""
    SELECT M."ItemCode",M."ItemName",TO_DATE(M."CreateDate") AS "CreateDate",
        M."U_Sub_Group" AS "SubGroup",M."U_TYPE" AS "ItemType",
        G."ItmsGrpNam" AS "Category",ROUND(M."LastPurPrc",4) AS "LastPrice",
        ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "TotalOnHand",
        ROUND(COALESCE(SUM(W."OnOrder"),0),0) AS "TotalOnOrder",
        ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),2) AS "StockValue"
    FROM {db}.OITM M JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN {db}.OITW W ON M."ItemCode"=W."ItemCode"
    WHERE M."ItemCode"='{s}'
    GROUP BY M."ItemCode",M."ItemName",M."CreateDate",M."U_Sub_Group",M."U_TYPE",G."ItmsGrpNam",M."LastPurPrc" """)})

@app.get("/api/trace_log")
def trace_log(item:str=Query(""),days:int=Query(0),schema:str=Query("jivo_oil"),month:str=Query(None)):
    db=get_schema(schema);s=safe(item)
    date_f=f"AND TO_CHAR(N.\"DocDate\",'YYYY-MM')='{safe(month)}'" if month else (f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})" if days>0 else "")
    return JSONResponse(content={"data":q(f"""
    SELECT N."TransNum",N."TransType",CAST(N."BASE_REF" AS VARCHAR(50)) AS "BaseRef",
        TO_DATE(N."DocDate") AS "DocDate",N."CardName",N."JrnlMemo",N."Comments",
        ROUND(N."InQty",3) AS "InQty",ROUND(N."OutQty",3) AS "OutQty",
        ROUND(N."Price",4) AS "Price",ROUND(N."TransValue",2) AS "TransValue",
        N."Warehouse",COALESCE(H."WhsName",N."Warehouse") AS "WhsName",ROUND(N."Balance",3) AS "Balance"
    FROM {db}.OINM N LEFT JOIN {db}.OWHS H ON N."Warehouse"=H."WhsCode"
    WHERE N."ItemCode"='{s}' AND N."TransType" NOT IN (14,16) {date_f}
    ORDER BY N."DocDate" DESC,N."TransNum" DESC""")})

@app.get("/api/trace_returns")
def trace_returns(item:str=Query(""),days:int=Query(0),schema:str=Query("jivo_oil"),month:str=Query(None)):
    db=get_schema(schema);s=safe(item)
    date_f=f"AND TO_CHAR(N.\"DocDate\",'YYYY-MM')='{safe(month)}'" if month else (f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})" if days>0 else "")
    return JSONResponse(content={"data":q(f"""
    SELECT N."TransNum",N."TransType",TO_DATE(N."DocDate") AS "DocDate",N."CardName",
        N."JrnlMemo",N."Comments",ROUND(N."InQty",3) AS "ReturnQty",
        ROUND(N."TransValue",2) AS "TransValue",N."Warehouse",COALESCE(H."WhsName",N."Warehouse") AS "WhsName",
        CASE N."TransType" WHEN 14 THEN 'AR Return' WHEN 16 THEN 'AR Credit Note' END AS "ReturnType"
    FROM {db}.OINM N LEFT JOIN {db}.OWHS H ON N."Warehouse"=H."WhsCode"
    WHERE N."ItemCode"='{s}' AND N."TransType" IN (14,16) AND N."InQty">0 {date_f}
    ORDER BY N."DocDate" DESC""")})

@app.get("/api/trace_disassembly")
def trace_disassembly(item:str=Query(""),days:int=Query(0),schema:str=Query("jivo_oil"),month:str=Query(None)):
    db=get_schema(schema);s=safe(item)
    date_f=f"AND TO_CHAR(W.\"StartDate\",'YYYY-MM')='{safe(month)}'" if month else (f"AND W.\"StartDate\">=ADD_DAYS(CURRENT_DATE,-{days})" if days>0 else "")
    return JSONResponse(content={"data":q(f"""
    SELECT W."DocNum",W."Status",TO_DATE(W."StartDate") AS "StartDate",TO_DATE(W."DueDate") AS "DueDate",
        TO_DATE(W."CloseDate") AS "CloseDate",ROUND(W."PlannedQty",2) AS "PlannedQty",ROUND(W."CmpltQty",2) AS "ActualQty",W."Comments"
    FROM {db}.OWOR W WHERE W."ItemCode"='{s}' AND W."Type"='D' {date_f} ORDER BY W."StartDate" DESC""")})

@app.get("/api/pm_bom")
def pm_bom(item:str=Query(""),schema:str=Query("jivo_oil")):
    db=get_schema(schema);s=safe(item)
    return JSONResponse(content={"data":q(f"""
    SELECT L."Father" AS "FGCode",M."ItemName" AS "FGName",
        COALESCE(M."U_Sub_Group",'–') AS "SubGroup",
        ROUND(L."Quantity",4) AS "QtyPerUnit",
        ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "FGOnHand",
        ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),0) AS "FGStockValue"
    FROM {db}.ITT1 L
    JOIN {db}.OITM M ON L."Father"=M."ItemCode"
    LEFT JOIN {db}.OITW W ON L."Father"=W."ItemCode"
    WHERE L."Code"='{s}'
    GROUP BY L."Father",M."ItemName",M."U_Sub_Group",L."Quantity"
    ORDER BY "FGOnHand" DESC""")})

@app.get("/api/pm_consumption")
def pm_consumption(item:str=Query(""),schema:str=Query("jivo_oil")):
    db=get_schema(schema);s=safe(item)
    return JSONResponse(content={"data":q(f"""
    SELECT TO_CHAR(N."DocDate",'YYYY-MM') AS "Month",
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
    ORDER BY "Month" DESC""")})

@app.get("/api/pm_invoices")
def pm_invoices(item:str=Query(""),schema:str=Query("jivo_oil")):
    db=get_schema(schema);s=safe(item)
    return JSONResponse(content={"data":q(f"""
    SELECT I."DocNum",TO_DATE(I."DocDate") AS "DocDate",
        I."CardName" AS "Supplier",
        ROUND(L."Quantity",0) AS "Qty",
        ROUND(L."Price",4) AS "UnitPrice",
        ROUND(L."LineTotal",0) AS "LineTotal",
        CASE I."DocStatus" WHEN 'O' THEN 'Open' WHEN 'C' THEN 'Closed' ELSE I."DocStatus" END AS "Status"
    FROM {db}.OPDN I
    JOIN {db}.PDN1 L ON I."DocEntry"=L."DocEntry"
    WHERE L."ItemCode"='{s}' AND I."CANCELED"='N'
    ORDER BY I."DocDate" DESC""")})

@app.get("/api/pm_summary")
def pm_summary(item:str=Query(""),schema:str=Query("jivo_oil"),period:int=Query(12)):
    db=get_schema(schema);s=safe(item)
    if period>0:
        before_p=f'N."DocDate" < ADD_MONTHS(CURRENT_DATE,{-period})'
        in_p=f'AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE,{-period})'
        bill_p=f'AND I."DocDate" >= ADD_MONTHS(CURRENT_DATE,{-period})'
    else:
        before_p='1=0';in_p='';bill_p=''
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
    bil=q(f"""
    SELECT COALESCE(COUNT(DISTINCT I."DocNum"),0) AS "InvoiceCount",
           ROUND(COALESCE(SUM(L."Quantity"),0),0) AS "BilledQty",
           ROUND(COALESCE(SUM(L."LineTotal"),0),0) AS "BilledValue",
           COALESCE(COUNT(DISTINCT I."CardCode"),0) AS "SupplierCount"
    FROM {db}.OPCH I JOIN {db}.PCH1 L ON I."DocEntry"=L."DocEntry"
    WHERE L."ItemCode"='{s}' AND I."CANCELED"='N' {bill_p}""")
    cls=q(f"""
    SELECT ROUND(COALESCE(SUM(W."OnHand"),0),0) AS "ClosingQty",
           ROUND(COALESCE(SUM(W."OnHand"*M."LastPurPrc"),0),0) AS "ClosingValue"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"
    WHERE W."ItemCode"='{s}'""")
    whs=q(f"""
    SELECT W."WhsCode", WH."WhsName", ROUND(W."OnHand",0) AS "OnHand"
    FROM {db}.OITW W JOIN {db}.OWHS WH ON W."WhsCode"=WH."WhsCode"
    WHERE W."ItemCode"='{s}' AND W."OnHand"!=0
    ORDER BY W."OnHand" DESC""")
    warehouses=[{"WhsCode":r["WhsCode"],"WhsName":r["WhsName"],"OnHand":round(float(r.get("OnHand") or 0),0)} for r in whs]
    return JSONResponse(content={
        "movements":mvt[0] if mvt else {},
        "billing":bil[0] if bil else {},
        "closing":cls[0] if cls else {},
        "warehouses":warehouses})

@app.get("/api/fg_pm_summary")
def fg_pm_summary(item:str=Query(""),schema:str=Query("jivo_oil"),period:int=Query(12)):
    db=get_schema(schema);s=safe(item)
    if period>0:
        before_p=f'N."DocDate" < ADD_MONTHS(CURRENT_DATE,{-period})'
        in_p=f'AND N."DocDate" >= ADD_MONTHS(CURRENT_DATE,{-period})'
    else:
        before_p='1=0';in_p=''
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
    # Add per-FG warehouse breakdown
    items=list(r)
    if items:
        fg_in="','".join([safe(i["FGCode"]) for i in items])
        whs=q(f"""
        SELECT W."ItemCode" AS "FGCode",W."WhsCode",WH."WhsName",ROUND(W."OnHand",0) AS "OnHand"
        FROM {db}.OITW W JOIN {db}.OWHS WH ON W."WhsCode"=WH."WhsCode"
        WHERE W."ItemCode" IN ('{fg_in}') AND W."OnHand"!=0
        ORDER BY W."OnHand" DESC""")
        whs_map={}
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

@app.get("/api/planning")
def planning(subgroup:str=Query(None),item_type:str=Query(None),schema:str=Query("jivo_oil")):
    db=get_schema(schema)
    sg=f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else ""
    type_f=tf(item_type)
    return JSONResponse(content={"data":q(f"""
    WITH CONSUMPTION AS (
        SELECT N."ItemCode",
            SUM(CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-30) THEN N."OutQty" ELSE 0 END) AS "Out30d",
            SUM(CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-60) AND N."DocDate"<ADD_DAYS(CURRENT_DATE,-30) THEN N."OutQty" ELSE 0 END) AS "Out30_60d",
            SUM(CASE WHEN N."DocDate">=ADD_DAYS(CURRENT_DATE,-90) THEN N."OutQty" ELSE 0 END) AS "Out90d",
            MAX(CASE WHEN N."OutQty">0 THEN N."DocDate" END) AS "LastMoveDate"
        FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
        WHERE N."OutQty">0 AND N."DocDate">=ADD_DAYS(CURRENT_DATE,-90) AND I."U_Unit"='OIL'
        GROUP BY N."ItemCode"
    )
    SELECT M."ItemCode",M."ItemName",COALESCE(M."U_Sub_Group",'–') AS "SubGroup",COALESCE(M."U_TYPE",'–') AS "ItemType",
        ROUND(SUM(W."OnHand"),0) AS "TotalOnHand",ROUND(SUM(W."OnHand"*M."LastPurPrc"),0) AS "StockValue",
        ROUND(COALESCE(C."Out30d",0),0) AS "Out30d",ROUND(COALESCE(C."Out30_60d",0),0) AS "Out30_60d",
        ROUND(COALESCE(C."Out90d",0)/90,1) AS "AvgDailyOut",ROUND(COALESCE(C."Out90d",0)/3,0) AS "AvgMonthlyOut",
        CASE WHEN COALESCE(C."Out90d",0)=0 THEN -1
             ELSE ROUND(SUM(W."OnHand")/(COALESCE(C."Out90d",0)/90),0) END AS "DaysOfStockLeft",
        -- SuggestedOrder: how many units needed to bring stock up to 30-day supply
        CASE WHEN COALESCE(C."Out90d",0)=0 THEN 0
             WHEN SUM(W."OnHand")<(COALESCE(C."Out90d",0)/90)*30
             THEN ROUND(((COALESCE(C."Out90d",0)/90)*30)-SUM(W."OnHand"),0)
             ELSE 0 END AS "SuggestedOrder",
        CASE WHEN COALESCE(C."Out30d",0)=0 AND COALESCE(C."Out30_60d",0)=0 THEN 'FLAT'
             WHEN COALESCE(C."Out30d",0)>COALESCE(C."Out30_60d",0)*1.1 THEN 'RISING'
             WHEN COALESCE(C."Out30d",0)<COALESCE(C."Out30_60d",0)*0.9 THEN 'FALLING'
             ELSE 'STABLE' END AS "Trend",
        TO_DATE(C."LastMoveDate") AS "LastMoveDate",TO_DATE(M."CreateDate") AS "CreateDate"
    FROM {db}.OITM M JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN {db}.OITW W ON M."ItemCode"=W."ItemCode"
    LEFT JOIN CONSUMPTION C ON M."ItemCode"=C."ItemCode"
    WHERE G."ItmsGrpNam"='FINISHED' AND M."InvntItem"='Y' AND M."U_Unit"='OIL' AND W."OnHand">0
      AND M."U_Sub_Group" IN ({FG_VALID}) {sg} {type_f}
    GROUP BY M."ItemCode",M."ItemName",M."U_Sub_Group",M."U_TYPE",C."Out30d",C."Out30_60d",C."Out90d",C."LastMoveDate",M."CreateDate"
    ORDER BY "DaysOfStockLeft" ASC""")})

@app.post("/api/chat")
async def chat(request:Request):
    if not GROQ_API_KEY: return JSONResponse(content={"reply":"Set GROQ_API_KEY."})
    try:
        body=await request.json();msg=body.get("message","");ctx=body.get("context","")
        import httpx
        async with httpx.AsyncClient(timeout=60) as cl:
            r=await cl.post("https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                json={"model":"llama-3.3-70b-versatile","max_tokens":1024,"messages":[
                    {"role":"system","content":f"Inventory analyst for Jivo Oil. Data:\n{ctx}\nBe concise. Indian format (Cr/L)."},
                    {"role":"user","content":msg}]})
            d=r.json();ch=d.get("choices",[])
            return JSONResponse(content={"reply":ch[0]["message"]["content"] if ch else d.get("error",{}).get("message","Error")})
    except Exception as e: return JSONResponse(content={"reply":str(e)})

@app.get("/",response_class=HTMLResponse)
async def serve():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"templates","dashboard_oils.html"),"r",encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/conveyor",response_class=HTMLResponse)
async def conveyor():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"conveyor_sample.html"),"r",encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

if __name__=="__main__":
    uvicorn.run(app,host="0.0.0.0",port=8004)

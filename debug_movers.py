import os, sys
sys.path.append(os.getcwd())

from main_backend_oils import get_schema, whs_f, cf, tf, safe

db = get_schema("jivo_oil")
days = 30
category = None
subgroup = "OLIVE"
item_type = None
whs = None
date_from = None
date_to = None

wf2 = whs_f(whs)
f = cf(category)
type_f = tf(item_type)
GIFT_EXCL = "AND M.\"U_Sub_Group\" != 'GIFT PACK'"

date_filter=f"AND N.\"DocDate\">=ADD_DAYS(CURRENT_DATE,-{days})"
day_div=str(days)

# movers_by_subgroup SQL
q_sg = f"""
    SELECT COALESCE(CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE M."U_Sub_Group" END,'UNCLASSIFIED') AS "SubGroup",
        COUNT(DISTINCT M."ItemCode") AS "TotalSKUs",
        COUNT(DISTINCT CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN M."ItemCode" END) AS "NonMovingSKUs",
        COUNT(DISTINCT CASE WHEN COALESCE(MV."TotalOut",0)>0 AND COALESCE(MV."TotalOut",0)<50 THEN M."ItemCode" END) AS "SlowSKUs",
        COUNT(DISTINCT CASE WHEN COALESCE(MV."TotalOut",0)>=50 AND COALESCE(MV."TotalOut",0)<500 THEN M."ItemCode" END) AS "MediumSKUs",
        COUNT(DISTINCT CASE WHEN COALESCE(MV."TotalOut",0)>=500 THEN M."ItemCode" END) AS "FastSKUs",
        ROUND(SUM(W."StockValue"),2) AS "StockValue",
        ROUND(SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN W."StockValue" ELSE 0 END),2) AS "StuckValue"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN (SELECT N."ItemCode",SUM(N."OutQty") AS "TotalOut" FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
        WHERE N."OutQty">0 {date_filter} AND I."U_Unit"='OIL' GROUP BY N."ItemCode") MV ON M."ItemCode"=MV."ItemCode"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {wf2} {type_f} {GIFT_EXCL}
    GROUP BY CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE M."U_Sub_Group" END
"""

# movers SQL
sg="AND M.\"U_Sub_Group\"='MUSTARD' AND M.\"U_Type\"='PREMIUM'" if subgroup=='YELLOW MUSTARD' else (f"AND M.\"U_Sub_Group\"='{safe(subgroup)}'" if subgroup else "")

q_m = f"""
    SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",
        CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE COALESCE(M."U_Sub_Group",'?"') END AS "SubGroup",
        COALESCE(M."U_TYPE",'?"') AS "ItemType",
        ROUND(SUM(W."OnHand"),2) AS "TotalOnHand",ROUND(SUM(W."StockValue"),2) AS "StockValue",
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
"""

from db import q

rows_sg = q(q_sg)
for r in rows_sg:
    if r['SubGroup'] == 'OLIVE':
        print("SG Total SKUs:", r['TotalSKUs'])

rows_m = q(q_m)
print("Movers count:", len(rows_m))

items_sg = set()
q_items_sg = f"""
    SELECT DISTINCT M."ItemCode"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {f} AND W."OnHand">0 {wf2} {type_f} {GIFT_EXCL} AND M."U_Sub_Group"='OLIVE'
"""
for r in q(q_items_sg):
    items_sg.add(r['ItemCode'])

items_m = set([r['ItemCode'] for r in rows_m])

missing = items_sg - items_m
print("Items in SG but not in Movers:", missing)

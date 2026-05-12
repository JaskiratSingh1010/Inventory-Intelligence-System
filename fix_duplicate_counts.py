import re

# Fix main_backend_oils.py
with open("main_backend_oils.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace the body of movers_by_subgroup
old_body = """    db=get_schema(schema);type_f=tf(item_type);wf2=whs_f(whs)
    cat_f=f"AND G.\\"ItmsGrpNam\\"='{category.upper()}'" if category else ""
    # Use same logic as cf() function for consistency
    if category and category.upper() in ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL','TRADING ITEMS','GIFT PACK'):
        valid = f"AND G.\\"ItmsGrpNam\\"='{category.upper()}'"
    else:
        valid = "AND G.\\"ItmsGrpNam\\" IN ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL','TRADING ITEMS','GIFT PACK')"
    date_filter=f"AND N.\\"DocDate\\">='{date_from}' AND N.\\"DocDate\\"<='{date_to}'" if date_from and date_to else f"AND N.\\"DocDate\\">=ADD_DAYS(CURRENT_DATE,-{days})"
    return JSONResponse(content={"data":q(f\"\"\"
    SELECT COALESCE(CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE M."U_Sub_Group" END,'UNCLASSIFIED') AS "SubGroup",
        COUNT(DISTINCT M."ItemCode") AS "TotalSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN 1 ELSE 0 END) AS "NonMovingSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>0 AND COALESCE(MV."TotalOut",0)<50 THEN 1 ELSE 0 END) AS "SlowSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>=50 AND COALESCE(MV."TotalOut",0)<500 THEN 1 ELSE 0 END) AS "MediumSKUs",
        SUM(CASE WHEN COALESCE(MV."TotalOut",0)>=500 THEN 1 ELSE 0 END) AS "FastSKUs",
        ROUND(SUM(W."StockValue"),2) AS "StockValue",
        ROUND(SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN W."StockValue" ELSE 0 END),0) AS "StuckValue"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    LEFT JOIN (SELECT N."ItemCode",SUM(N."OutQty") AS "TotalOut" FROM {db}.OINM N JOIN {db}.OITM I ON N."ItemCode"=I."ItemCode"
        WHERE N."OutQty">0 {date_filter} AND I."U_Unit"='OIL' GROUP BY N."ItemCode") MV ON M."ItemCode"=MV."ItemCode"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' {cat_f} AND W."OnHand">0 {wf2} AND M."U_Sub_Group" IN ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL','TRADING ITEMS','GIFT PACK') {type_f}
    GROUP BY CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE M."U_Sub_Group" END
    ORDER BY "StuckValue" DESC,"StockValue" DESC\"\"\")})"""

new_body = """    db=get_schema(schema);type_f=tf(item_type);wf2=whs_f(whs);f=cf(category)
    date_filter=f"AND N.\\"DocDate\\">='{date_from}' AND N.\\"DocDate\\"<='{date_to}'" if date_from and date_to else f"AND N.\\"DocDate\\">=ADD_DAYS(CURRENT_DATE,-{days})"
    return JSONResponse(content={"data":q(f\"\"\"
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
    ORDER BY "StuckValue" DESC,"StockValue" DESC\"\"\")})"""

content = content.replace(old_body, new_body)

# Fix movers_summary stuck value rounding
content = re.sub(r'ROUND\(SUM\(CASE WHEN COALESCE\(MV\."TotalOut",0\)=0 THEN W\."StockValue" ELSE 0 END\),0\)', r'ROUND(SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN W."StockValue" ELSE 0 END),2)', content)

# Fix movers endpoint unused vars
old_movers_vars = """    # Use same logic as cf() function for consistency
    if category and category.upper() in ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL','TRADING ITEMS','GIFT PACK'):
        valid = f"AND G.\\"ItmsGrpNam\\"='{category.upper()}'"
    else:
        valid = "AND G.\\"ItmsGrpNam\\" IN ('FINISHED','RAW MATERIAL','PACKAGING MATERIAL','TRADING ITEMS','GIFT PACK')"
    cat_f=f"AND G.\\"ItmsGrpNam\\"='{category.upper()}'" if category else ""
"""
content = content.replace(old_movers_vars, "    f=cf(category)\n")

with open("main_backend_oils.py", "w", encoding="utf-8") as f:
    f.write(content)

# Fix main_backend_beverages.py
with open("main_backend_beverages.py", "r", encoding="utf-8") as f:
    content = f.read()

content = content.replace('SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN 1 ELSE 0 END) AS "NonMovingSKUs"', 'COUNT(DISTINCT CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN M."ItemCode" END) AS "NonMovingSKUs"')
content = content.replace('SUM(CASE WHEN COALESCE(MV."TotalOut",0)>0 AND COALESCE(MV."TotalOut",0)<50 THEN 1 ELSE 0 END) AS "SlowSKUs"', 'COUNT(DISTINCT CASE WHEN COALESCE(MV."TotalOut",0)>0 AND COALESCE(MV."TotalOut",0)<50 THEN M."ItemCode" END) AS "SlowSKUs"')
content = content.replace('SUM(CASE WHEN COALESCE(MV."TotalOut",0)>=50 AND COALESCE(MV."TotalOut",0)<500 THEN 1 ELSE 0 END) AS "MediumSKUs"', 'COUNT(DISTINCT CASE WHEN COALESCE(MV."TotalOut",0)>=50 AND COALESCE(MV."TotalOut",0)<500 THEN M."ItemCode" END) AS "MediumSKUs"')
content = content.replace('SUM(CASE WHEN COALESCE(MV."TotalOut",0)>=500 THEN 1 ELSE 0 END) AS "FastSKUs"', 'COUNT(DISTINCT CASE WHEN COALESCE(MV."TotalOut",0)>=500 THEN M."ItemCode" END) AS "FastSKUs"')
content = content.replace('ROUND(SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN W."StockValue" ELSE 0 END),0) AS "StuckValue"', 'ROUND(SUM(CASE WHEN COALESCE(MV."TotalOut",0)=0 THEN W."StockValue" ELSE 0 END),2) AS "StuckValue"')

with open("main_backend_beverages.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Fixed counting duplicated rows in movers_by_subgroup!")

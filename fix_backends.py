import re
import os

oils_path = r"c:\Users\JIVO\Downloads\Inventory-Intelligence-System-main\Inventory-Intelligence-System-main\main_backend_oils.py"
bev_path = r"c:\Users\JIVO\Downloads\Inventory-Intelligence-System-main\Inventory-Intelligence-System-main\main_backend_beverages.py"

def fix_oils():
    with open(oils_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. kpi
    content = content.replace(
        'ROUND((SELECT SUM(W."OnHand") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"=\'Y\' {f} AND W."OnHand">0 {wf_}),0)',
        'ROUND((SELECT SUM(W."OnHand") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}),2)'
    )
    content = content.replace(
        'ROUND(COALESCE((SELECT SUM(N."TransValue") FROM {db}.OINM N JOIN {db}.OITM M ON N."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE 1=1 {f} {n_wf(wf_)}),0),0)',
        'ROUND(COALESCE((SELECT SUM(W."StockValue") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}),0),2)'
    )
    content = content.replace(
        '(SELECT COUNT(DISTINCT W."ItemCode") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"=\'Y\' {f} AND W."OnHand">0 {wf_})',
        '(SELECT COUNT(DISTINCT W."ItemCode") FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' {f} AND W."OnHand">0 {wf_} {GIFT_EXCL})'
    )
    content = content.replace(
        '(SELECT COUNT(*) FROM (SELECT M."ItemCode" FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"=\'Y\' {f} {wf_} GROUP BY M."ItemCode" HAVING SUM(W."OnHand")<=0))',
        '(SELECT COUNT(*) FROM (SELECT M."ItemCode" FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod" WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' {f} {wf_} {GIFT_EXCL} GROUP BY M."ItemCode" HAVING SUM(W."OnHand")<=0))'
    )

    # 2. categories
    content = content.replace(
        'SELECT G."ItmsGrpNam" AS "Category",COUNT(DISTINCT W."ItemCode") AS "SKUs",ROUND(SUM(W."OnHand"),0) AS "Qty",ROUND(SUM(V."Val"),0) AS "Value"\n    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"\n    JOIN (SELECT "ItemCode", "Warehouse", SUM("TransValue") as "Val" FROM {db}.OINM GROUP BY "ItemCode", "Warehouse") V ON W."ItemCode"=V."ItemCode" AND W."WhsCode"=V."Warehouse"\n    WHERE M."InvntItem"=\'Y\' AND G."ItmsGrpNam" IN (\'FINISHED\',\'RAW MATERIAL\',\'PACKAGING MATERIAL\',\'TRADING ITEMS\',\'GIFT PACK\') AND W."OnHand">0',
        'SELECT G."ItmsGrpNam" AS "Category",COUNT(DISTINCT W."ItemCode") AS "SKUs",ROUND(SUM(W."OnHand"),2) AS "Qty",ROUND(SUM(W."StockValue"),2) AS "Value"\n    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"\n    WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' AND G."ItmsGrpNam" IN (\'FINISHED\',\'RAW MATERIAL\',\'PACKAGING MATERIAL\',\'TRADING ITEMS\',\'GIFT PACK\') AND W."OnHand">0 {GIFT_EXCL}'
    )

    # 3. out_of_stock
    content = content.replace(
        'SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",ROUND(SUM(W."OnHand"),0) AS "TotalOnHand"\n    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"\n    WHERE M."InvntItem"=\'Y\' {f}',
        'SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",ROUND(SUM(W."OnHand"),2) AS "TotalOnHand"\n    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"\n    WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' {f} {GIFT_EXCL}'
    )

    # 4. warehouse_summary
    content = content.replace(
        'ROUND(SUM(W."OnHand"),0) AS "Qty",ROUND(SUM(W."OnOrder"),0) AS "OnOrder",\n        ROUND(SUM(V."Val"),0) AS "Value"\n    FROM {db}.OITW W\n    JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"\n    JOIN {db}.OWHS H ON W."WhsCode"=H."WhsCode"\n    LEFT JOIN {db}.OUSR U ON CAST(H."U_Owner" AS VARCHAR(20))=CAST(U."USERID" AS VARCHAR(20))\n    JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"\n    JOIN (SELECT "ItemCode", "Warehouse", SUM("TransValue") as "Val" FROM {db}.OINM GROUP BY "ItemCode", "Warehouse") V ON W."ItemCode"=V."ItemCode" AND W."WhsCode"=V."Warehouse"\n    WHERE M."InvntItem"=\'Y\' {f} AND W."OnHand">0 {owner_f}',
        'ROUND(SUM(W."OnHand"),2) AS "Qty",ROUND(SUM(W."OnOrder"),2) AS "OnOrder",\n        ROUND(SUM(W."StockValue"),2) AS "Value"\n    FROM {db}.OITW W\n    JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode"\n    JOIN {db}.OWHS H ON W."WhsCode"=H."WhsCode"\n    LEFT JOIN {db}.OUSR U ON CAST(H."U_Owner" AS VARCHAR(20))=CAST(U."USERID" AS VARCHAR(20))\n    JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"\n    WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' {f} AND W."OnHand">0 {owner_f} {GIFT_EXCL}'
    )

    # 5. warehouse_items
    content = content.replace(
        'ROUND(W."StockValue",2) AS "StockValue"\n    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"\n    JOIN (SELECT "ItemCode", "Warehouse", SUM("TransValue") as "Val" FROM {db}.OINM GROUP BY "ItemCode", "Warehouse") V ON W."ItemCode"=V."ItemCode" AND W."WhsCode"=V."Warehouse"\n    WHERE M."InvntItem"=\'Y\' {f} AND W."WhsCode"=\'{s}\' AND W."OnHand">0',
        'ROUND(W."StockValue",2) AS "StockValue"\n    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"\n    WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' {f} AND W."WhsCode"=\'{s}\' AND W."OnHand">0 {GIFT_EXCL}'
    )

    # 6. stock_position
    content = content.replace(
        'WHERE M."InvntItem"=\'Y\' {f} AND W."OnHand">0 {wf_}',
        'WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'OIL\' {f} AND W."OnHand">0 {wf_} {GIFT_EXCL}'
    )

    # Convert all ROUND(...,0) to ROUND(...,2) for stock and value
    content = re.sub(r'ROUND\((.*?W\."OnHand".*?),\s*0\)', r'ROUND(\1,2)', content)
    content = re.sub(r'ROUND\((.*?W\."StockValue".*?),\s*0\)', r'ROUND(\1,2)', content)

    with open(oils_path, "w", encoding="utf-8") as f:
        f.write(content)


def fix_bev():
    with open(bev_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Apply Precision (ROUND to 2)
    content = re.sub(r'ROUND\((.*?W\."OnHand".*?),\s*0\)', r'ROUND(\1,2)', content)
    content = re.sub(r'ROUND\((.*?W\."StockValue".*?),\s*0\)', r'ROUND(\1,2)', content)
    content = re.sub(r'ROUND\((.*?TotalQty.*?),\s*0\)', r'ROUND(\1,2)', content)
    content = re.sub(r'ROUND\((.*?TotalValue.*?),\s*0\)', r'ROUND(\1,2)', content)

    # Fix trace endpoints to use BEV_XX_VALID
    content = content.replace(
        'cat_f = f"AND G.\\"ItmsGrpNam\\"=\'{cat}\'" if cat in (\'RAW MATERIAL\',\'PACKAGING MATERIAL\') else "AND G.\\"ItmsGrpNam\\"=\'FINISHED\'"\n    seen = set(); result = []',
        'cat_f = f"AND G.\\"ItmsGrpNam\\"=\'{cat}\'" if cat in (\'RAW MATERIAL\',\'PACKAGING MATERIAL\') else "AND G.\\"ItmsGrpNam\\"=\'FINISHED\'"\n    valid = BEV_FG_VALID if cat == \'FINISHED\' else (BEV_PM_VALID if cat == \'PACKAGING MATERIAL\' else BEV_RM_VALID)\n    seen = set(); result = []'
    )
    content = content.replace(
        'AND M."U_Sub_Group" IS NOT NULL AND M."U_Sub_Group"!=\'\'',
        'AND M."U_Sub_Group" IN ({valid})'
    )

    content = content.replace(
        'cat_f = f"AND G.\\"ItmsGrpNam\\"=\'{cat}\'" if cat in (\'RAW MATERIAL\',\'PACKAGING MATERIAL\') else "AND G.\\"ItmsGrpNam\\"=\'FINISHED\'"\n    sg = f"AND M.\\"U_Sub_Group\\"=\'{safe(subgroup)}\'" if subgroup else ""',
        'cat_f = f"AND G.\\"ItmsGrpNam\\"=\'{cat}\'" if cat in (\'RAW MATERIAL\',\'PACKAGING MATERIAL\') else "AND G.\\"ItmsGrpNam\\"=\'FINISHED\'"\n    valid = BEV_FG_VALID if cat == \'FINISHED\' else (BEV_PM_VALID if cat == \'PACKAGING MATERIAL\' else BEV_RM_VALID)\n    sg = f"AND M.\\"U_Sub_Group\\"=\'{safe(subgroup)}\'" if subgroup else ""'
    )
    content = content.replace(
        'WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'{UNIT}\' {cat_f} {sg}',
        'WHERE M."InvntItem"=\'Y\' AND M."U_Unit"=\'{UNIT}\' {cat_f} AND M."U_Sub_Group" IN ({valid}) {sg}'
    )

    # Fix ABC XYZ Subgroup filtering
    content = content.replace(
        'FROM ABC_BASE A LEFT JOIN XYZ_BASE X ON A."ItemCode"=X."ItemCode" GROUP BY A."SubGroup")',
        'FROM ABC_BASE A LEFT JOIN XYZ_BASE X ON A."ItemCode"=X."ItemCode" WHERE A."SubGroup" IN ({BEV_FG_VALID}) GROUP BY A."SubGroup")'
    )

    # Fix Planning endpoint filtering
    content = content.replace(
        'WHERE G."ItmsGrpNam"=\'FINISHED\' AND M."InvntItem"=\'Y\' AND M."U_Unit"=\'{UNIT}\' AND W."OnHand">0 {sg}',
        'WHERE G."ItmsGrpNam"=\'FINISHED\' AND M."InvntItem"=\'Y\' AND M."U_Unit"=\'{UNIT}\' AND W."OnHand">0 AND M."U_Sub_Group" IN ({BEV_FG_VALID}) {sg}'
    )

    with open(bev_path, "w", encoding="utf-8") as f:
        f.write(content)

fix_oils()
fix_bev()
print("Fixed successfully.")

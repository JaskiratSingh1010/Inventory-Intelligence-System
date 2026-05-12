import os, sys
sys.path.append(os.getcwd())
from main_backend_oils import get_schema, whs_f, cf, tf, safe, q

db = get_schema('jivo_oil')
print('Schema:', db)

q_sg = f'''
    SELECT COALESCE(CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE M."U_Sub_Group" END,'UNCLASSIFIED') AS "SubGroup",
        COUNT(DISTINCT M."ItemCode") AS "TotalSKUs"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' AND W."OnHand">0 AND M."U_Sub_Group" != 'GIFT PACK'
    GROUP BY CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE M."U_Sub_Group" END
'''

rows_sg = q(q_sg)
for r in rows_sg:
    if r['SubGroup'] == 'OLIVE':
        print('SG Total SKUs:', r['TotalSKUs'])

q_m = f'''
    SELECT G."ItmsGrpNam" AS "Category",M."ItemCode",M."ItemName",
        CASE WHEN M."U_Sub_Group"='MUSTARD' AND M."U_TYPE"='PREMIUM' THEN 'YELLOW MUSTARD' ELSE COALESCE(M."U_Sub_Group",'?') END AS "SubGroup"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' AND W."OnHand">0 AND M."U_Sub_Group"='OLIVE' AND M."U_Sub_Group" != 'GIFT PACK'
    GROUP BY G."ItmsGrpNam",M."ItemCode",M."ItemName",M."U_Sub_Group",M."U_TYPE"
'''

rows_m = q(q_m)
print('Movers count:', len(rows_m))

q_all = f'''
    SELECT M."ItemCode"
    FROM {db}.OITW W JOIN {db}.OITM M ON W."ItemCode"=M."ItemCode" JOIN {db}.OITB G ON M."ItmsGrpCod"=G."ItmsGrpCod"
    WHERE M."InvntItem"='Y' AND M."U_Unit"='OIL' AND W."OnHand">0 AND M."U_Sub_Group"='OLIVE' AND M."U_Sub_Group" != 'GIFT PACK'
'''
all_items = set([r['ItemCode'] for r in q(q_all)])
print('Total matching items in OITW:', len(all_items))

import os

with open('main_backend_oils.py', 'r', encoding='utf-8') as f:
    c = f.read()

c = c.replace(
    'SELECT COALESCE(CASE WHEN M."U_Sub_Group"=\'MUSTARD\' AND M."U_TYPE"=\'PREMIUM\' THEN \'YELLOW MUSTARD\' ELSE M."U_Sub_Group" END,\'UNCLASSIFIED\') AS "SubGroup",',
    'SELECT G."ItmsGrpNam" AS "Category", COALESCE(CASE WHEN M."U_Sub_Group"=\'MUSTARD\' AND M."U_TYPE"=\'PREMIUM\' THEN \'YELLOW MUSTARD\' ELSE M."U_Sub_Group" END,\'UNCLASSIFIED\') AS "SubGroup",'
)
c = c.replace(
    'GROUP BY CASE WHEN M."U_Sub_Group"=\'MUSTARD\' AND M."U_TYPE"=\'PREMIUM\' THEN \'YELLOW MUSTARD\' ELSE M."U_Sub_Group" END',
    'GROUP BY G."ItmsGrpNam", CASE WHEN M."U_Sub_Group"=\'MUSTARD\' AND M."U_TYPE"=\'PREMIUM\' THEN \'YELLOW MUSTARD\' ELSE M."U_Sub_Group" END'
)

with open('main_backend_oils.py', 'w', encoding='utf-8') as f:
    f.write(c)


with open('main_backend_beverages.py', 'r', encoding='utf-8') as f:
    cb = f.read()

cb = cb.replace(
    'SELECT COALESCE(M."U_Sub_Group",\'UNCLASSIFIED\') AS "SubGroup",',
    'SELECT G."ItmsGrpNam" AS "Category", COALESCE(M."U_Sub_Group",\'UNCLASSIFIED\') AS "SubGroup",'
)
cb = cb.replace(
    'GROUP BY M."U_Sub_Group" ORDER BY "StuckValue" DESC,"StockValue" DESC',
    'GROUP BY G."ItmsGrpNam", M."U_Sub_Group" ORDER BY "StuckValue" DESC,"StockValue" DESC'
)

# Also update the loop in main_backend_beverages.py
cb = cb.replace(
    '''            for r in rows:
                sg = r["SubGroup"]
                if sg not in agg:
                    agg[sg] = {"SubGroup": sg, "TotalSKUs": 0, "NonMovingSKUs": 0, "SlowSKUs": 0, "MediumSKUs": 0, "FastSKUs": 0, "StockValue": 0, "StuckValue": 0}''',
    '''            for r in rows:
                sg = r["SubGroup"]
                if sg not in agg:
                    agg[sg] = {"SubGroup": sg, "Category": r.get("Category", ""), "TotalSKUs": 0, "NonMovingSKUs": 0, "SlowSKUs": 0, "MediumSKUs": 0, "FastSKUs": 0, "StockValue": 0, "StuckValue": 0}'''
)

with open('main_backend_beverages.py', 'w', encoding='utf-8') as f:
    f.write(cb)

print("Backend category grouping updated")

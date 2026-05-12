import os

with open('main_backend_beverages.py', 'r', encoding='utf-8') as f:
    c = f.read()

c = c.replace('def movers_by_subgroup(days: int = Query(30), category: str = Query("FINISHED"),', 'def movers_by_subgroup(days: int = Query(30), category: str = Query(None),')
c = c.replace("cat = (category or 'FINISHED').upper()", "cat = category.upper() if category else ''")
c = c.replace('cat_f = f"AND G.\\"ItmsGrpNam\\"=\'{cat}\'"', 'cat_f = f"AND G.\\"ItmsGrpNam\\"=\'{cat}\'" if category else "AND G.\\"ItmsGrpNam\\" IN (\'FINISHED\',\'RAW MATERIAL\',\'PACKAGING MATERIAL\')"')

with open('main_backend_beverages.py', 'w', encoding='utf-8') as f:
    f.write(c)
print('Beverages backend updated')

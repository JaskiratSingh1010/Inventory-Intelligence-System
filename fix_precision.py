import os
import re

for filename in ["main_backend_oils.py", "main_backend_beverages.py"]:
    filepath = os.path.join(r"c:\Users\JIVO\Downloads\Inventory-Intelligence-System-main\Inventory-Intelligence-System-main", filename)
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    content = re.sub(r',\s*0\)\s*AS\s*"ClosingValue"', r',2) AS "ClosingValue"', content)
    content = re.sub(r',\s*0\)\s*AS\s*"OpeningValue"', r',2) AS "OpeningValue"', content)
    content = re.sub(r',\s*0\)\s*AS\s*"PurchaseValue"', r',2) AS "PurchaseValue"', content)
    content = re.sub(r',\s*0\)\s*AS\s*"ConsumptionValue"', r',2) AS "ConsumptionValue"', content)
    content = re.sub(r',\s*0\)\s*AS\s*"BilledValue"', r',2) AS "BilledValue"', content)
    
    # Also change the rounding in the Python aggregation code
    content = re.sub(r'totals\[k\]=round\(totals\[k\],0\)', r'totals[k]=round(totals[k],2)', content)
    content = re.sub(r'round\(v\[k\],0\)', r'round(v[k],2)', content)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
print("Precision fixed.")

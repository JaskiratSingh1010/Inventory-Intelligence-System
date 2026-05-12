import os

for filename in ["main_backend_oils.py", "main_backend_beverages.py"]:
    filepath = os.path.join(r"c:\Users\JIVO\Downloads\Inventory-Intelligence-System-main\Inventory-Intelligence-System-main", filename)
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    content = content.replace('NULLIF(SUM(SUM(W."StockValue")) OVER(),2)', 'NULLIF(SUM(SUM(W."StockValue")) OVER(),0)')

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
print("Fixed NULLIF.")

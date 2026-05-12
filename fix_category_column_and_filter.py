import os

# ─── OILS DASHBOARD ───────────────────────────────────────────────────────────
with open('templates/dashboard_oils.html', 'r', encoding='utf-8') as f:
    oils = f.read()

# 1. Add "Category" header after Sub-Group header in renderMoTable
oils = oils.replace(
    "     <th style=\"width:24px\"></th><th>Sub-Group</th>\n     <th class=\"R\">Total SKUs",
    "     <th style=\"width:24px\"></th><th>Sub-Group</th>\n     <th style=\"color:var(--mt);font-weight:700\">Category</th>\n     <th class=\"R\">Total SKUs"
)

# 2. Add "Category" data cell after SubGroup data cell in each row
oils = oils.replace(
    "       <td style=\"font-weight:700\">${v.SubGroup}</td>\n       <td class=\"R m\">${N(v.TotalSKUs)}",
    "       <td style=\"font-weight:700\">${v.SubGroup}</td>\n       <td style=\"font-size:11px;color:var(--mt);font-weight:600\">${v.Category||''}</td>\n       <td class=\"R m\">${N(v.TotalSKUs)}"
)

# 3. Fix TOTAL row colspan (was 3, needs to be 4 now with Category column)
oils = oils.replace(
    "    html += '<td colspan=\"3\"><b>TOTAL</b> <span style=\"color:var(--mt);font-weight:600\">(' + moSGData.length + ' groups)</span></td>';",
    "    html += '<td colspan=\"4\"><b>TOTAL</b> <span style=\"color:var(--mt);font-weight:600\">(' + moSGData.length + ' groups)</span></td>';"
)

# 4. Fix cpls onclick to also clear moRawCache
oils = oils.replace(
    "  moItemCache={};biItemCache={};abItemCache={};whItemCache={};load()\n  });",
    "  moItemCache={};moRawCache={};biItemCache={};abItemCache={};whItemCache={};load()\n  });"
)

with open('templates/dashboard_oils.html', 'w', encoding='utf-8') as f:
    f.write(oils)

print("Oils dashboard updated")

# ─── BEVERAGES DASHBOARD ──────────────────────────────────────────────────────
with open('templates/dashboard_beverages.html', 'r', encoding='utf-8') as f:
    bev = f.read()

# Fix cpls onclick to also clear moRawCache (beverages doesn't have moRawCache, but safety)
bev = bev.replace(
    "moItemCache = {}; biItemCache = {}; abItemCache = {}; whItemCache = {}; load()",
    "moItemCache = {}; biItemCache = {}; abItemCache = {}; whItemCache = {}; moItemCache = {}; load()"
)

with open('templates/dashboard_beverages.html', 'w', encoding='utf-8') as f:
    f.write(bev)

print("Beverages dashboard updated")
print("Done!")

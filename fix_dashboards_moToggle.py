import os

with open(r'templates\dashboard_oils.html', 'r', encoding='utf-8') as f:
    c = f.read()

# Fix moToggleSG to pass correct category and pass item_type (t)
old_api_call = "const d=await api(`/api/movers?days=${moDays}&category=${encodeURIComponent(cat)}&subgroup=${encodeURIComponent(sg)}${t}${moDtP2}${whsP}`);"
new_api_call = "const catP = C ? `&category=${encodeURIComponent(C)}` : '';\n    const d=await api(`/api/movers?days=${moDays}${catP}&subgroup=${encodeURIComponent(sg)}${t}${moDtP2}${whsP}`);"
c = c.replace(old_api_call, new_api_call)

with open(r'templates\dashboard_oils.html', 'w', encoding='utf-8') as f:
    f.write(c)

with open(r'templates\dashboard_beverages.html', 'r', encoding='utf-8') as f:
    cb = f.read()

# Fix moToggleSG to pass item_type (t) if it exists
old_bev_api = "const d = await api('/api/movers?days=' + moDays + (C ? '&category=' + encodeURIComponent(C) : '') + '&subgroup=' + encodeURIComponent(sg) + dateP + statusP + whsP);"
new_bev_api = "const t = (typeof moType !== 'undefined' && moType !== 'all') ? '&item_type=' + moType : '';\n        const d = await api('/api/movers?days=' + moDays + (C ? '&category=' + encodeURIComponent(C) : '') + '&subgroup=' + encodeURIComponent(sg) + dateP + statusP + whsP + t);"
cb = cb.replace(old_bev_api, new_bev_api)

with open(r'templates\dashboard_beverages.html', 'w', encoding='utf-8') as f:
    f.write(cb)

print("Dashboards moToggleSG fixed!")

import os

with open(r'templates\dashboard_beverages.html', 'r', encoding='utf-8') as f:
    c = f.read()

c = c.replace("const cat = C || 'FINISHED';\n      const t = '';", "const catP = C ? '&category=' + encodeURIComponent(C) : '';\n      const t = '';")
c = c.replace("moSGData = await api('/api/movers_by_subgroup?days=' + moDays + moDtP + whsP);", "moSGData = await api('/api/movers_by_subgroup?days=' + moDays + moDtP + whsP + catP);")
c = c.replace("const d = await api('/api/movers?days=' + moDays + '&category=' + encodeURIComponent(cat)", "const d = await api('/api/movers?days=' + moDays + (C ? '&category=' + encodeURIComponent(C) : '')")

with open(r'templates\dashboard_beverages.html', 'w', encoding='utf-8') as f:
    f.write(c)

with open(r'templates\dashboard_oils.html', 'r', encoding='utf-8') as f:
    c = f.read()

c = c.replace("moSGData=await api(`/api/movers_by_subgroup?days=${moDays}${t}${moDtP}${whsP}`);", "const catP = C ? `&category=${encodeURIComponent(C)}` : '';\n    moSGData=await api(`/api/movers_by_subgroup?days=${moDays}${t}${moDtP}${whsP}${catP}`);")
c = c.replace("api(`/api/movers?days=${moDays}&category=${encodeURIComponent(C||'FINISHED')}", "api(`/api/movers?days=${moDays}` + (C ? `&category=${encodeURIComponent(C)}` : ``)")

with open(r'templates\dashboard_oils.html', 'w', encoding='utf-8') as f:
    f.write(c)
print('Dashboards updated')

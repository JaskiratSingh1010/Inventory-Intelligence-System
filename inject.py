import re

print('Starting clean injection...')

# 1. Read files
with open('conveyor_sample.html', 'r', encoding='utf-8') as f:
    cv = f.read()
with open('templates/dashboard_oils.html', 'r', encoding='utf-8') as f:
    db = f.read()

# 2. Extract the full inner content of factory-bg
# Find the start tag
start_tag = '<div class="factory-bg">'
start_idx = cv.find(start_tag)
if start_idx == -1:
    print("ERROR: factory-bg not found!")
    exit(1)

# Walk from start_tag to find matching closing </div>
pos = start_idx + len(start_tag)
depth = 1
while pos < len(cv) and depth > 0:
    open_pos = cv.find('<div', pos)
    close_pos = cv.find('</div>', pos)
    if close_pos == -1:
        break
    if open_pos != -1 and open_pos < close_pos:
        depth += 1
        pos = open_pos + 4
    else:
        depth -= 1
        if depth == 0:
            end_idx = close_pos + 6  # include </div>
        pos = close_pos + 6

# Extract just the inner content (without the outer factory-bg div tags)
inner_start = start_idx + len(start_tag)
inner_end = end_idx - 6  # before the last </div>
inner_html = cv[inner_start:inner_end].strip()

print(f"Extracted inner HTML: {len(inner_html)} chars")

# 3. Also extract the status-bar and info-bar
statusbar_start = cv.find('<div class="status-bar">')
statusbar_end = cv.find('</div>', statusbar_start) + 6
statusbar_html = cv[statusbar_start:statusbar_end].strip()

infobar_start = cv.find('<div class="info-bar">')
infobar_end = cv.find('</div>', infobar_start) + 6
# info-bar has nested divs, need to find actual end
pos2 = infobar_start + len('<div class="info-bar">')
depth2 = 1
while pos2 < len(cv) and depth2 > 0:
    open_pos2 = cv.find('<div', pos2)
    close_pos2 = cv.find('</div>', pos2)
    if close_pos2 == -1:
        break
    if open_pos2 != -1 and open_pos2 < close_pos2:
        depth2 += 1
        pos2 = open_pos2 + 4
    else:
        depth2 -= 1
        if depth2 == 0:
            infobar_end = close_pos2 + 6
        pos2 = close_pos2 + 6
infobar_html = cv[infobar_start:infobar_end].strip()

print(f"Status bar: {len(statusbar_html)} chars, Info bar: {len(infobar_html)} chars")

# 4. Build the replacement animation block
#    We use transform:scale on the 900px inner div but set outer height via JS
#    The outer has overflow:hidden, width:100%, no padding, no flex centering
animation_block = f"""
   <!-- OVERHEAD FACTORY ANIMATION -->
   <div id="factoryAnimWrap" style="width:100%; border-radius:12px; overflow:hidden; margin-bottom:16px; position:relative;">
     <div id="factoryAnimInner" style="width:900px; transform-origin:top left; background:linear-gradient(180deg,#7c83b8 0%,#a4a8ce 35%,#d1c8d5 65%,#fbdba6 100%); border-radius:12px; overflow:hidden;">
       {inner_html}
       {statusbar_html}
       {infobar_html}
     </div>
   </div>
"""

# 5. Replace the existing (broken) wrapper in dashboard_oils.html
# Find start of <!-- OVERHEAD FACTORY ANIMATION --> and the closing </div> of factoryAnimWrap
overhead_start = db.find('<!-- OVERHEAD FACTORY ANIMATION -->')
if overhead_start == -1:
    print("No existing animation block, inserting before dK...")
    db = db.replace('<div id="dK"></div>', animation_block + '\n   <div id="dK"></div>')
else:
    # Find the closing </div> of factoryAnimWrap
    wrap_start = db.find('<div id="factoryAnimWrap"', overhead_start)
    pos3 = wrap_start + len('<div id="factoryAnimWrap"')
    # skip to end of opening tag
    pos3 = db.find('>', pos3) + 1
    depth3 = 1
    while pos3 < len(db) and depth3 > 0:
        open_p = db.find('<div', pos3)
        close_p = db.find('</div>', pos3)
        if close_p == -1:
            break
        if open_p != -1 and open_p < close_p:
            depth3 += 1
            pos3 = open_p + 4
        else:
            depth3 -= 1
            if depth3 == 0:
                wrap_end = close_p + 6
            pos3 = close_p + 6
    
    old_block = db[overhead_start:wrap_end]
    db = db.replace(old_block, animation_block.strip())
    print(f"Replaced existing block ({len(old_block)} chars)")

# 6. Fix/add the resizer JS  - replace old resFactory if present
resize_js = """
  // ── RESPONSIVE FACTORY ANIMATION SCALER ──
  function resFactory() {
    const wrap = document.getElementById('factoryAnimWrap');
    const inner = document.getElementById('factoryAnimInner');
    if (!wrap || !inner) return;
    const scale = wrap.offsetWidth / 900;
    inner.style.transform = 'scale(' + scale + ')';
    wrap.style.height = (260 * scale) + 'px';
  }
  setTimeout(resFactory, 50);
  window.addEventListener('resize', resFactory);
  // Override toggleSide to trigger resize after transition
  const _origToggleSide = typeof toggleSide === 'function' ? toggleSide : null;
  if (_origToggleSide) {
    toggleSide = function() { _origToggleSide(); setTimeout(resFactory, 320); };
  }
"""

# Remove old resFactory block if present
if 'function resFactory()' in db:
    # Find and remove old block
    old_res_start = db.find('// Responsive scaler for the factory animation')
    if old_res_start == -1:
        old_res_start = db.find('// ── RESPONSIVE FACTORY ANIMATION SCALER ──')
    if old_res_start != -1:
        old_res_end = db.find('\n  });', old_res_start)
        if old_res_end == -1:
            old_res_end = db.find('\n  };\n', old_res_start + 200) + 5
        db = db[:old_res_start] + db[old_res_end:]
        print("Removed old resFactory block")

# Add new resize JS before last </script>
last_script_end = db.rfind('</script>')
if last_script_end != -1:
    db = db[:last_script_end] + resize_js + '\n</script>' + db[last_script_end+9:]
    print("Added resize JS")

# Write back
with open('templates/dashboard_oils.html', 'w', encoding='utf-8') as f:
    f.write(db)

print('Done! Dashboard updated successfully.')

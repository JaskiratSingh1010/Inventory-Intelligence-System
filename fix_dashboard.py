import re

print('Restoring dashboard_oils.html to clean state...')

with open('templates/dashboard_oils.html', 'r', encoding='utf-8') as f:
    db = f.read()

# 1. Remove the injected factory animation HTML block
overhead_start = db.find('<!-- OVERHEAD FACTORY ANIMATION -->')
if overhead_start != -1:
    # Find the closing </div> of factoryAnimWrap (depth counting)
    wrap_start = db.find('<div id="factoryAnimWrap"', overhead_start)
    pos = db.find('>', wrap_start) + 1
    depth = 1
    while pos < len(db) and depth > 0:
        op = db.find('<div', pos)
        cp = db.find('</div>', pos)
        if cp == -1:
            break
        if op != -1 and op < cp:
            depth += 1
            pos = op + 4
        else:
            depth -= 1
            if depth == 0:
                wrap_end = cp + 6
            pos = cp + 6
    
    block = db[overhead_start:wrap_end]
    db = db.replace(block, '')
    print(f'Removed animation HTML block ({len(block)} chars)')

# 2. Remove injected factory CSS (two copies)
css_marker = '/* ── FACTORY ANIMATION CSS ── */'
while css_marker in db:
    css_start = db.find(css_marker)
    # Find the next occurrence of the original </style> that was before this block
    # The CSS was injected BEFORE </style>, so it ends right before </style>
    # Find: from css_start, find first </style>
    css_end = db.find('</style>', css_start)
    db = db[:css_start] + '</style>' + db[css_end+8:]
    print(f'Removed a factory CSS block')

# 3. Remove injected factory JS <script> block
js_marker = '// ── FACTORY ANIMATION JS ──'
if js_marker in db:
    js_start = db.rfind('<script>', 0, db.find(js_marker)) 
    js_end = db.find('</script>', js_start) + 9
    db = db[:js_start] + db[js_end:]
    print(f'Removed factory JS block')

# 4. Now inject cleanly using an iframe
iframe_block = """
   <!-- OVERHEAD FACTORY ANIMATION (iframe - isolated) -->
   <div style="width:100%; margin-bottom:16px; border-radius:12px; overflow:hidden; border:1px solid var(--bd); box-shadow:0 2px 12px rgba(0,0,0,0.08); line-height:0;">
     <iframe src="/static/conveyor_sample.html" 
             id="factoryIframe"
             style="width:100%; height:220px; border:none; display:block;" 
             scrolling="no"
             title="Production Line Animation">
     </iframe>
   </div>
"""

db = db.replace('<div id="dK"></div>', iframe_block + '\n   <div id="dK"></div>')
print('Injected iframe block')

with open('templates/dashboard_oils.html', 'w', encoding='utf-8') as f:
    f.write(db)

print('Done! Dashboard restored and iframe injected.')

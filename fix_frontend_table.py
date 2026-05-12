import os

def process_html(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        c = f.read()

    # Add the column header
    c = c.replace(
        "h += '<th>Sub-Group</th>';",
        "h += '<th>Sub-Group</th>';\n      h += '<th style=\"color:var(--mt);font-weight:700\">Category</th>';"
    )

    # Add the column data
    c = c.replace(
        "h += '<td style=\"font-weight:700\">' + v.SubGroup + '</td>';",
        "h += '<td style=\"font-weight:700\">' + v.SubGroup + '</td>';\n          h += '<td style=\"font-size:11px;color:var(--mt);font-weight:600\">' + (v.Category || '') + '</td>';"
    )
    
    # Update colspan for the loading row
    c = c.replace('<td colspan="10"><div class="cat-items-inner"', '<td colspan="11"><div class="cat-items-inner"')

    # Fix the filters (the buttons circled in red)
    # Let's inspect what the issue is. The pills at the top right are probably:
    # <button class="pl" data-c="FINISHED">Finished</button>
    # They should have an onclick event to set C and reload the table.
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(c)

process_html('templates/dashboard_oils.html')
process_html('templates/dashboard_beverages.html')
print("HTML tables updated")

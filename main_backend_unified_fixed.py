import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import os

# Import all the functions from both backends
from main_backend_oils import (
    kpi as oils_kpi, categories as oils_categories, 
    out_of_stock as oils_out_of_stock, stock_position as oils_stock_position,
    movement as oils_movement, movers_summary as oils_movers_summary,
    movers_by_subgroup as oils_movers_by_subgroup, movers as oils_movers,
    not_billed_summary as oils_not_billed_summary, not_billed as oils_not_billed,
    abcxyz_summary as oils_abcxyz_summary, abcxyz as oils_abcxyz,
    trace_items as oils_trace_items, trace_log as oils_trace_log,
    aging as oils_aging, pm_bom as oils_pm_bom, pm_consumption as oils_pm_consumption,
    warehouse_summary as oils_warehouse_summary, warehouses as oils_warehouses,
    warehouse_owners as oils_warehouse_owners
)

from main_backend_beverages import (
    kpi as beverages_kpi, categories as beverages_categories,
    out_of_stock as beverages_out_of_stock, stock_position as beverages_stock_position,
    movement as beverages_movement, movers_summary as beverages_movers_summary,
    movers_by_subgroup as beverages_movers_by_subgroup, movers as beverages_movers,
    not_billed_summary as beverages_not_billed_summary, not_billed as beverages_not_billed,
    abcxyz_summary as beverages_abcxyz_summary, abcxyz as beverages_abcxyz,
    trace_items as beverages_trace_items, trace_log as beverages_trace_log,
    aging as beverages_aging, pm_bom as beverages_pm_bom, pm_consumption as beverages_pm_consumption,
    warehouse_summary as beverages_warehouse_summary, warehouses as beverages_warehouses,
    warehouse_owners as beverages_warehouse_owners
)

# Unified Application
app = FastAPI(title="Jivo Unified Inventory Dashboard")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Serve bundled JS libraries locally (no CDN dependency)
_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# Unified API endpoints
@app.get("/api/kpi")
def unified_kpi(category: str = Query(None), division: str = Query("oils"), whs: str = Query(None)):
    if division == "oils":
        return oils_kpi(category=category, schema="jivo_oil", whs=whs)
    else:
        return beverages_kpi(category=category, whs=whs)

@app.get("/api/categories")
def unified_categories(category: str = Query(None), division: str = Query("oils")):
    if division == "oils":
        return oils_categories(schema="jivo_oil")
    else:
        return beverages_categories(category=category)

@app.get("/api/movers_summary")
def unified_movers_summary(days: int = Query(30), category: str = Query(None), 
                           date_from: str = Query(None), date_to: str = Query(None), 
                           whs: str = Query(None), division: str = Query("oils")):
    if division == "oils":
        return oils_movers_summary(days=days, category=category, schema="jivo_oil", 
                              date_from=date_from, date_to=date_to, whs=whs)
    else:
        return beverages_movers_summary(days=days, category=category, 
                                  date_from=date_from, date_to=date_to, whs=whs)

@app.get("/api/movers")
def unified_movers(days: int = Query(30), category: str = Query(None), subgroup: str = Query(None),
                item_type: str = Query(None), status: str = Query(None),
                date_from: str = Query(None), date_to: str = Query(None), 
                whs: str = Query(None), page: int = Query(1), limit: int = Query(100),
                division: str = Query("oils")):
    if division == "oils":
        return oils_movers(days=days, category=category, subgroup=subgroup, item_type=item_type,
                        date_from=date_from, date_to=date_to, whs=whs, 
                        page=page, limit=limit, schema="jivo_oil")
    else:
        return beverages_movers(days=days, category=category, subgroup=subgroup, status=status,
                           date_from=date_from, date_to=date_to, whs=whs, 
                           page=page, limit=limit)

@app.get("/api/stock_position")
def unified_stock_position(category: str = Query(None), division: str = Query("oils"), whs: str = Query(None)):
    if division == "oils":
        return oils_stock_position(category=category, schema="jivo_oil", whs=whs)
    else:
        return beverages_stock_position(category=category, whs=whs)

# Dashboard Routes
@app.get("/oils/", response_class=HTMLResponse)
async def serve_oils():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "dashboard_oils.html"), "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/beverages/", response_class=HTMLResponse)
async def serve_beverages():
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "dashboard_beverages.html"), "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/", include_in_schema=False)
def index():
    return RedirectResponse(url="/oils/")

if __name__ == "__main__":
    print("Starting Unified Jivo Dashboard on port 8005...")
    uvicorn.run(app, host="0.0.0.0", port=8005)

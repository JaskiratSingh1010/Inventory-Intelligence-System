import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import os
import pandas as pd
import traceback

# Import working oils backend app directly
from main_backend_oils import app as oils_app

# Working Application
app = FastAPI(title="Jivo Working Dashboard")

# Serve static and template files
_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
_templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")
if os.path.isdir(_templates_dir):
    app.mount("/templates", StaticFiles(directory=_templates_dir), name="templates")

# Mount the oils app with all its endpoints under /oils/
app.mount("/oils", oils_app)

@app.get("/", include_in_schema=False)
def index():
    return RedirectResponse(url="/templates/dashboard_oils.html")

if __name__ == "__main__":
    print("Starting Working Jivo Dashboard on port 8005...")
    uvicorn.run(app, host="192.168.1.171", port=8005)

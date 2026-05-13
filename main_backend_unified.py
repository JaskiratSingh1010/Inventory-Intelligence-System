import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
import os

from main_backend_oils import app as oils_app
from main_backend_beverages import app as beverages_app

# Unified Application
app = FastAPI(title="Jivo Unified Inventory Dashboard")

# Serve bundled JS libraries locally (no CDN dependency)
_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# Mount the separate division apps
app.mount("/oils", oils_app)
app.mount("/beverages", beverages_app)

@app.get("/", include_in_schema=False)
def index():
    # Redirect root to Oils by default
    return RedirectResponse(url="/oils/")

if __name__ == "__main__":
    print("Starting Unified Jivo Dashboard on port 8005...")
    uvicorn.run(app, host="127.0.0.1", port=8005)

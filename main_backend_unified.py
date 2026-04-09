import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from main_backend_oils import app as oils_app
from main_backend_beverages import app as beverages_app

# Unified Application
app = FastAPI(title="Jivo Unified Inventory Dashboard")

# Mount the separate division apps
app.mount("/oils", oils_app)
app.mount("/beverages", beverages_app)

@app.get("/", include_in_schema=False)
def index():
    # Redirect root to Oils by default
    return RedirectResponse(url="/oils/")

if __name__ == "__main__":
    print("Starting Unified Jivo Dashboard on port 8004...")
    uvicorn.run(app, host="192.168.1.68", port=8005)

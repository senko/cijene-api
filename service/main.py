from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging
import os

from service.routers import v0
from service.config import settings

os.makedirs(settings.archive_dir, exist_ok=True)
_service_log_file = os.path.join(settings.archive_dir, "service.log")
_service_handler = logging.FileHandler(_service_log_file)
_service_level = logging.DEBUG if settings.debug else logging.INFO
_service_handler.setLevel(_service_level)
_service_handler.setFormatter(
    logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
)
root_logger = logging.getLogger()
root_logger.setLevel(_service_level)
root_logger.addHandler(_service_handler)

app = FastAPI(
    title="Cijene API",
    description="Service for product pricing data by Croatian grocery chains",
    version=settings.version,
    debug=settings.debug,
)

# Add CORS middleware to allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include versioned routers
app.include_router(v0.router, prefix="/v0")


@app.exception_handler(404)
async def custom_404_handler(request: Request, exc: HTTPException):
    """Custom 404 handler with helpful message directing to API docs."""
    return JSONResponse(
        status_code=404,
        content={"detail": "Resource not found. Check documentation at /docs"},
    )


@app.get("/")
async def root():
    """Root endpoint redirects to main website."""
    return RedirectResponse(url=settings.redirect_url, status_code=302)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(
        "service.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )

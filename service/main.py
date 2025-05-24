from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.exceptions import HTTPException
import uvicorn

from service.routers import v0
from service.config import settings

app = FastAPI(
    title="Cijene API",
    description="Service for product pricing data by Croatian grocery chains",
    version=settings.version,
    debug=settings.debug,
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

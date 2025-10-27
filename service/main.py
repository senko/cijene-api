from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.exceptions import HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from service.routers import v0, v1
from service.config import settings

db = settings.get_db()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to handle startup and shutdown events."""
    await db.connect()
    await db.create_tables()
    yield
    await db.close()


app = FastAPI(
    title="Cijene API",
    description="Service for product pricing data by Croatian grocery chains",
    version=settings.version,
    debug=settings.debug,
    root_path=settings.root_path,
    lifespan=lifespan,
    openapi_components={
        "securitySchemes": {"HTTPBearer": {"type": "http", "scheme": "bearer"}}
    },
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
app.include_router(v1.router, prefix="/v1")


@app.exception_handler(404)
async def custom_404_handler(request: Request, exc: HTTPException):
    """Custom 404 handler with helpful message directing to API docs."""
    return JSONResponse(
        status_code=404,
        content={"detail": "Resource not found. Check documentation at /docs"},
    )


@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint redirects to main website."""
    return RedirectResponse(url=settings.redirect_url, status_code=302)


@app.get("/health", tags=["Service status"])
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


def main():
    log_level = logging.DEBUG if settings.debug else logging.INFO
    logging.basicConfig(level=log_level)
    uvicorn.run(
        "service.main:app",
        host=settings.host,
        port=settings.port,
        log_level=log_level,
        reload=settings.debug,
    )


if __name__ == "__main__":
    main()

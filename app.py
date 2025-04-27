"""
FastAPI application for Athena
"""
import logging
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from config.settings import Settings
from api.webhook import router as webhook_router
from api.analytics import router as analytics_router
from api.subscription import router as subscription_router
from api.advanced_analytics import router as advanced_analytics_router

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Athena API",
    description="API for Athena Telegram Bot for on-chain token analytics",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(webhook_router)
app.include_router(analytics_router)
app.include_router(subscription_router)
app.include_router(advanced_analytics_router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Welcome to Athena API"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


async def start_api_server(settings: Settings):
    """
    Start the API server
    
    Args:
        settings: Application settings
    """
    import uvicorn
    
    logger.info(f"Starting API server on {settings.api_host}:{settings.api_port}")
    
    config = uvicorn.Config(
        "api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        reload=False,
    )
    
    server = uvicorn.Server(config)
    try:
        await server.serve()
    except (OSError, SystemExit) as exc:
        logger.error(f"API server failed to start: {exc}")
        # Continue without API server

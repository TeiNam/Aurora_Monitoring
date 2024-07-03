import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from modules.mongodb_connector import MongoDBConnector
from modules.time_utils import get_kst_time
from config import (
    API_MAPPING, STATIC_FILES_DIR, TEMPLATES_DIR, HOST, PORT,
    ALLOWED_ORIGINS
)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await MongoDBConnector.initialize()
    logger.info(f"{get_kst_time()} - MongoDB connection initialized.")
    yield
    # Shutdown
    if MongoDBConnector.client:
        MongoDBConnector.client.close()
        logger.info(f"{get_kst_time()} - MongoDB connection closed.")

app = FastAPI(
    lifespan=lifespan,
    title="Monitoring Tool API",
    description="API for monitoring MySQL and managing related data",
    version="1.0.0",
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files setup
app.mount("/static", StaticFiles(directory=STATIC_FILES_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@app.get("/favicon.ico")
async def get_favicon():
    return Response(content="", media_type="image/x-icon")


@app.get("/", tags=["Health Check"])
async def health_check():
    try:
        db = await MongoDBConnector.get_database()
        await db.command('ping')
        return JSONResponse(content={"status": "healthy", "database": "connected"}, status_code=200)
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(content={"status": "unhealthy", "database": "disconnected"}, status_code=500)


@app.get("/sql-plan", tags=["UI"])
async def sql_explain(request: Request):
    return templates.TemplateResponse("sql_explain.html", {"request": request})


@app.get("/instance-setup", tags=["UI"])
async def instance_setup(request: Request):
    return templates.TemplateResponse("instance_setup.html", {"request": request})


@app.get("/memo", tags=["UI"])
async def memo_page(request: Request):
    return templates.TemplateResponse("memo.html", {"request": request})

# Mounting the APIs
for route, module_name in API_MAPPING.items():
    module = __import__(module_name, fromlist=['app'])
    app.mount(route, module.app)


# 전역 예외 처리기
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": exc.detail},
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error"},
    )

if __name__ == "__main__":
    uvicorn.run("apis:app", host=HOST, port=PORT, reload=True)
import uvicorn
from starlette.staticfiles import StaticFiles
from fastapi import FastAPI, Request
from fastapi.responses import Response, JSONResponse
from fastapi.templating import Jinja2Templates
from modules.mongodb_connector import MongoDBConnector
from modules.time_utils import get_kst_time

# APIs Mapping for Mounting
api_mapping = {
    "/api/instance_setup": "api.instance_setup_api",
    "/api/rds": "api.aurora_cluster_status_api",
    "/api/mysql_status": "api.mysql_com_status_api",
    "/api/mysql_slow_query": "api.mysql_slow_queries_api",
    "/api/mysql_explain": "api.mysql_slow_query_explain_api",
    "/api/memo": "api.memo_api",
    "/api/slow_query": "api.slow_query_stat_api",
}

app = FastAPI()
app.mount("/css", StaticFiles(directory="css"), name="css")
app.mount("/js", StaticFiles(directory="js"), name="js")
templates = Jinja2Templates(directory="templates")


@app.get("/favicon.ico")
async def get_favicon():
    return Response(content="", media_type="image/x-icon")


@app.get("/")
async def health_check():
    return JSONResponse(content={"status": "healthy"}, status_code=200)


@app.on_event("startup")
async def on_startup():
    await MongoDBConnector.initialize()
    print(f"{get_kst_time()} - MongoDB connection established successfully.")


@app.on_event("shutdown")
async def on_shutdown():
    if MongoDBConnector.client:
        await MongoDBConnector.client.close()
    print(f"{get_kst_time()} - MongoDB connection closed.")


@app.get("/sql-plan")
async def sql_explain(request: Request):
    return templates.TemplateResponse("sql_explain.html", {"request": request})


@app.get("/instance-setup")
async def instance_setup(request: Request):
    return templates.TemplateResponse("instance_setup.html", {"request": request})


@app.get("/memo")
async def memo_page(request: Request):
    return templates.TemplateResponse("memo.html", {"request": request})


# Mounting the APIs
for route, module_name in api_mapping.items():
    api_app = __import__(module_name, fromlist=['app']).app
    app.mount(route, api_app)

if __name__ == "__main__":
    uvicorn.run("apis:app", host="0.0.0.0", port=8000, reload=True)

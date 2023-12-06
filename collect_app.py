import asyncio
import uvicorn
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles

# 데이터 및 메트릭 수집 모듈
main_modules = [
    'getMysqlSlowQueries',
    'getRdsMetric',
    'getMysqlCommandStatus'
]

templates = Jinja2Templates(directory="templates")

app = FastAPI()

app.mount("/css", StaticFiles(directory="css"), name="css")

@app.on_event("startup")
async def startup_event():
    for module in main_modules:
        main = __import__(module, fromlist=["main"]).main
        asyncio.create_task(main())


@app.get("/sql-utility")
async def get_sql_utility(request: Request):
    return templates.TemplateResponse("sql_text_plan.html", {"request": request})


@app.get("/instance-registration")
async def get_sql_utility(request: Request):
    return templates.TemplateResponse("instance_registration.html", {"request": request})

# Mounting the APIs
for route, module_name in api_mapping.items():
    api_app = __import__(module_name, fromlist=['app']).app
    app.mount(route, api_app)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
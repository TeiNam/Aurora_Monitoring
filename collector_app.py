import asyncio
from collector.aurora_metrics import run_aurora_metrics
from collector.mysql_command_status import run_mysql_command_status
from collector.mysql_slow_queries import run_mysql_slow_queries


async def main():
    # 각 수집 앱의 메인 비동기 함수를 동시에 실행
    await asyncio.gather(
        run_aurora_metrics(),
        run_mysql_command_status(),
        run_mysql_slow_queries()
    )

if __name__ == '__main__':
    asyncio.run(main())


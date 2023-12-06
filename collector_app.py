import asyncio
import pytz
from collector.aurora_metrics import run_aurora_metrics
from collector.mysql_slow_queries import run_mysql_slow_queries
from collector.mysql_command_status import run_mysql_command_status
from datetime import datetime, timedelta
from modules.time_utils import get_kst_time


def get_seconds_until_midnight_kst():
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    tomorrow = now + timedelta(days=1)
    midnight_kst = datetime(
        year=tomorrow.year, month=tomorrow.month, day=tomorrow.day,
        hour=0, minute=0, second=0, tzinfo=kst
    )
    seconds_until_midnight = (midnight_kst - now).total_seconds()
    return seconds_until_midnight


async def run_daily_at_midnight(task_func):
    while True:
        seconds_until_midnight = get_seconds_until_midnight_kst()
        await asyncio.sleep(seconds_until_midnight)
        await task_func()


async def run_with_restart(task_func):
    while True:
        try:
            await task_func()
        except Exception as e:
            print(f"{get_kst_time()} - Error in {task_func.__name__}: {e}")
            print(f"{get_kst_time()} - Restarting task in 5 seconds...")
            await asyncio.sleep(5)  # 5초 후 재시작


async def run_periodically(task_func, interval_seconds):
    while True:
        await task_func()
        await asyncio.sleep(interval_seconds)


async def main():
    # aurora_metrics와 mysql_slow_queries는 예외 발생 시 재시작
    aurora_task = asyncio.create_task(run_with_restart(run_aurora_metrics))
    slow_queries_task = asyncio.create_task(run_with_restart(run_mysql_slow_queries))

    # mysql_command_status를 매일 자정에 실행
    command_status_task = asyncio.create_task(run_daily_at_midnight(run_mysql_command_status))

    await asyncio.gather(aurora_task, slow_queries_task, command_status_task)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"{get_kst_time()} - An error occurred: {e}")

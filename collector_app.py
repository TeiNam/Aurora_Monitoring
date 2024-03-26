import asyncio
import pytz
from collector.mysql_slow_queries import run_mysql_slow_queries
from collector.mysql_command_status import run_mysql_command_status
from collector.mysql_summary_by_digest import run_gather_digest
from collector.mysql_events_statements_hist import run_gather_history
from collector.aurora_cluster_info import get_aurora_info
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


# 자정에 한번 수집
async def run_daily_at_midnight(task_func):
    while True:
        seconds_until_midnight = get_seconds_until_midnight_kst()
        await asyncio.sleep(seconds_until_midnight)
        await task_func()


# 초단위 반복 수집
async def run_periodically(task_func, interval_seconds):
    while True:
        try:
            await task_func()
        except Exception as e:
            print(f"{get_kst_time()} - Error in {task_func.__name__}: {e}")
        await asyncio.sleep(interval_seconds)


async def run_with_restart(task_func):
    while True:
        try:
            await task_func()
        except Exception as e:
            print(f"{get_kst_time()} - Error in {task_func.__name__}: {e}")
            print(f"{get_kst_time()} - Restarting task in 5 seconds...")
            await asyncio.sleep(5)  # 5초 후 재시작


async def main():
    # aurora_metrics와 mysql_slow_queries는 예외 발생 시 재시작
    slow_queries_task = asyncio.create_task(run_with_restart(run_mysql_slow_queries))

    # mysql_command_status를 1시간 주기로 수집
    command_status_task = asyncio.create_task(run_periodically(run_mysql_command_status, 3600))

    # mysql_summary_by_digest 5분 주기로 수집
    digest_status_task = asyncio.create_task(run_periodically(run_gather_digest, 300))

    # mysql_summary_by_digest 1분 주기로 수집
    hist_status_task = asyncio.create_task(run_periodically(run_gather_history, 60))

    # get_aurora_task 15분 주기로 수집
    get_aurora_task = asyncio.create_task(run_periodically(get_aurora_info, 900))

    # 예외가 발생해도 다른 태스크에 영향을 주지 않도록 함
    await asyncio.gather(
        slow_queries_task,
        command_status_task,
        digest_status_task,
        hist_status_task,
        get_aurora_task,
        return_exceptions=True
    )

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"{get_kst_time()} - An error occurred: {e}")

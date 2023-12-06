from datetime import datetime, timezone, timedelta


def get_kst_time():
    utc_now = datetime.now(timezone.utc)
    kst = timezone(timedelta(hours=9))
    return utc_now.astimezone(kst).strftime("%Y-%m-%d %H:%M:%S KST")

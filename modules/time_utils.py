from datetime import datetime, timezone, timedelta


def get_kst_time():
    utc_now = datetime.now(timezone.utc)
    kst = timezone(timedelta(hours=9))
    return utc_now.astimezone(kst).strftime("%Y-%m-%d %H:%M:%S KST")


def convert_utc_to_kst(utc_time):
    if utc_time is None:
        return None
    kst = timezone(timedelta(hours=9))
    return utc_time.replace(tzinfo=timezone.utc).astimezone(kst)


def parse_datetime(date_string):
    try:
        return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def format_datetime(dt):
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d %H:%M:%S KST")
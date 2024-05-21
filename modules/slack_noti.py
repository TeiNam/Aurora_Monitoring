import requests
import os
from dotenv import load_dotenv

load_dotenv()

SLACK_API_TOKEN = os.getenv("SLACK_API_TOKEN")


def get_slack_user_id(email):
    headers = {
        'Authorization': f'Bearer {SLACK_API_TOKEN}',
        'Content-Type': 'application/json'
    }
    response = requests.get(f'https://slack.com/api/users.lookupByEmail?email={email}', headers=headers, verify=False)
    if response.status_code == 200 and response.json()['ok']:
        return response.json()['user']['id']
    return None


def send_slack_notification(user_email, title, instance_info, db_info, pid_info, execution_time):
    user_id = get_slack_user_id(user_email)
    if user_id is None:
        raise ValueError("User not found in Slack")

    formatted_message = (f'*{title}*\n<@{user_id}> 계정으로 실행한 SQL쿼리(PID: {pid_info})가\n '
                         f'*{instance_info}*, *{db_info}* DB에서 *{execution_time}* 초 동안 실행 되었습니다.\n'
                         ' 쿼리 검수 및 실행 시 주의가 필요합니다. \n'
                         f'http://{os.getenv("HOST")}:8000/sql-plan?pid={pid_info}')

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL is not set in the environment variables.")

    payload = {'text': formatted_message}
    response = requests.post(webhook_url, json=payload)

    if response.status_code != 200:
        raise ValueError(
            f"Request to slack returned an error {response.status_code}, the response is:\n{response.text}")

import os, requests, json


def get_lark_token():
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"

    payload = json.dumps({
        "app_id": os.getenv("LARK_APP_ID"),
        "app_secret": os.getenv("LARK_APP_SECRET")
    })
    headers = {
        'Content-Type': 'application/json',
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.json()['tenant_access_token']

    print("error form get_lark_token")
    raise Exception(response.json())

def send_message_to_lark(content):
    if not os.getenv("LARK_NOTIFICATION_CHAT_ID"):
        return None

    url = f"https://open.larksuite.com/open-apis/im/v1/messages?receive_id_type=chat_id"

    headers = {
        'Authorization': f'Bearer {get_lark_token()}',
    }

    payload = json.dumps(
        {
            "content": '{"text": "' + content + '"}',
            "msg_type": "text",
            "receive_id": os.getenv("LARK_NOTIFICATION_CHAT_ID")
        }
    )

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        return response.json()['data']

    return None

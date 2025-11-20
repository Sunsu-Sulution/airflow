import os, requests, json
import pandas as pd


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

def get_lark_base_to_df(app_id, table_id, filter_dict):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_id}/tables/{table_id}/records/search?page_size=500"

    headers = {
        'Authorization': f'Bearer {get_lark_token()}',
    }
    payload = json.dumps({
        "filter": filter_dict
    })

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        items = response.json()['data']['items']
        rows = []
        for item in items:
            fields = item.get("fields", {}) or {}
            row = {}
            for key, val in fields.items():
                if isinstance(val, str):
                    row[key] = val
                elif isinstance(val, list) and len(val) > 0:
                    first = val[0]
                    if isinstance(first, dict):
                        row[key] = first.get("text") or first.get("name") or first.get("value") or json.dumps(first,
                                                                                                              ensure_ascii=False)
                    else:
                        row[key] = str(first)
                elif isinstance(val, dict):
                    row[key] = val.get("text") or val.get("name") or val.get("value") or json.dumps(val,
                                                                                                    ensure_ascii=False)
                else:
                    row[key] = val
            rows.append(row)

        df = pd.DataFrame(rows)
        return df

    raise Exception(response.json())
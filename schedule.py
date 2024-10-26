import requests
import json

# 设置请求URL和headers
url = "https://api.coze.com/api/trigger/v1/webhook/biz_id/bot_platform/hook/1000000038778812676"
headers = {
    "Authorization": "Bearer iCTHEYll",
    "Content-Type": "application/json"
}

# 设置请求参数
data = {
    "count": 50
}

# 发送POST请求
response = requests.post(url, headers=headers, data=json.dumps(data))

# 打印响应
print(response.status_code)
print(response.text)


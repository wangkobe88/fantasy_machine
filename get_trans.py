import requests
from bs4 import BeautifulSoup
import pandas as pd

# 页面 URL
url = "https://geniidata.com/ordinals/address/bc1pzjdw683ll9d4e799mdkpjckf38tkdh55574ajpa6gspwjmhemw3q82h866?ref=6ARWR6&tab=transactions"

# 获取页面内容
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# 检查是否成功下载
if response.status_code == 200:
    content = response.text
else:
    content = f"Failed to download the page. Status code: {response.status_code}"

print(content)
# 查找包含数据的标签 (示例为 div 或 span)
rows = []
for row in soup.select("div.transaction-row"):  # 需要根据页面结构修改选择器
    columns = [col.get_text(strip=True) for col in row.find_all("span")]
    rows.append(columns)

print(rows)

# 创建 DataFrame 并保存为 CSV
df = pd.DataFrame(rows, columns=["列名1", "列名2", "列名3"])  # 修改列名
df.to_csv('transactions.csv', index=False)

print("数据已成功保存为 transactions.csv")

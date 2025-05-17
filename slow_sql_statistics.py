#!/usr/bin/env python3
# slow_sql_statistics.py
# 使用 DescribeSlowLogs API 查询 RDS 实例的慢日志统计情况

import json
import datetime
import requests
import sys
from collections import defaultdict

try:
    from aliyunsdkcore.client import AcsClient
    from aliyunsdkrds.request.v20140815.DescribeSlowLogsRequest import DescribeSlowLogsRequest
    from config import ACCESS_KEY_ID, ACCESS_KEY_SECRET, REGION_ID, DB_INSTANCE_ID, FEISHU_WEBHOOK
except ImportError as e:
    print(f"[ERROR] 导入依赖失败: {e}")
    print("[INFO] 请安装必要的依赖: pip install aliyun-python-sdk-core aliyun-python-sdk-rds requests")
    sys.exit(1)

# === 显示配置信息（敏感信息部分隐藏）===
print(f"[DEBUG] 区域: {REGION_ID}")
print(f"[DEBUG] 实例ID: {DB_INSTANCE_ID}")
print(f"[DEBUG] ACCESS_KEY_ID: {ACCESS_KEY_ID[:4]}{'*' * (len(ACCESS_KEY_ID) - 8)}{ACCESS_KEY_ID[-4:]}")
print(f"[DEBUG] FEISHU_WEBHOOK: {'已配置' if FEISHU_WEBHOOK and FEISHU_WEBHOOK != 'YOUR_FEISHU_WEBHOOK_URL' else '未配置'}")

# === 获取查询时间范围 ===
end_time = datetime.datetime.now()
start_time = end_time - datetime.timedelta(days=7)  # 默认查询最近7天
start_str = start_time.strftime("%Y-%m-%dZ")  # UTC 格式，与 API 要求保持一致
end_str = end_time.strftime("%Y-%m-%dZ")      # UTC 格式，与 API 要求保持一致

# === 初始化阿里云客户端 ===
try:
    client = AcsClient(ACCESS_KEY_ID, ACCESS_KEY_SECRET, REGION_ID)
    print("[INFO] 成功初始化阿里云客户端")
except Exception as e:
    print(f"[ERROR] 初始化阿里云客户端失败: {e}")
    sys.exit(1)

# === 构建请求 ===
request = DescribeSlowLogsRequest()
request.set_DBInstanceId(DB_INSTANCE_ID)
request.set_StartTime(start_str)
request.set_EndTime(end_str)
# 可选参数：设置排序键
request.set_SortKey("TotalExecutionCounts")  # 按总执行次数排序
# 可选参数：设置数据库名
# request.set_DBName("your_db_name")
request.set_PageSize(100)  # 每页返回的记录数
request.set_accept_format('json')

print(f"[INFO] 查询时间范围: {start_str} ~ {end_str}")

# === 发送请求并获取所有记录（分页处理） ===
all_slow_logs = []
page_number = 1
max_pages = 20  # 最多获取20页数据
total_records = 0

try:
    # 分页查询所有慢查询统计记录
    while page_number <= max_pages:
        request.set_PageNumber(page_number)
        response = client.do_action_with_exception(request)
        print(f"[INFO] 成功发送第{page_number}页请求至阿里云")
        result = json.loads(response)
        # 获取页面信息
        total_records = result.get('TotalRecordCount', 0)
        items = result.get("Items", {})
        page_slow_logs = items.get("SQLSlowLog", [])
        
        if not page_slow_logs:
            print(f"[INFO] 第{page_number}页没有找到慢查询统计记录")
            break
        
        all_slow_logs.extend(page_slow_logs)
        print(f"[INFO] 已累计获取 {len(all_slow_logs)} 条慢查询统计记录")
        
        # 如果当前页记录数小于页大小，说明已经是最后一页
        if len(page_slow_logs) < 100:
            break
            
        page_number += 1
    
    if not all_slow_logs:
        print("[WARN] 没有找到满足条件的慢查询统计记录")
        sys.exit(0)
    
    print(f"[INFO] 共获取到 {len(all_slow_logs)} 条慢查询统计记录")
    
except Exception as e:
    print(f"[ERROR] 调用阿里云 DescribeSlowLogs API 失败: {e}")
    sys.exit(1)

# === 处理并显示结果 ===
# 按SQL模板的执行次数排序
sorted_slow_logs = sorted(all_slow_logs, key=lambda x: int(x.get('MySQLTotalExecutionCounts', 0)), reverse=True)

# 生成 Markdown 表格内容
markdown_table = "### 慢查询统计报告\n\n"
markdown_table += f"**查询时间范围**: {start_time.strftime('%Y-%m-%d')} 至 {end_time.strftime('%Y-%m-%d')}\n\n"
markdown_table += "| # | 数据库 | SQL模板 | 执行次数 | 平均执行时间(ms) | 最大执行时间(ms) | 解析行数(总计) | 扫描行数(最大) |\n"
markdown_table += "|---|--------|---------|----------|----------------|----------------|--------------|----------------|\n"

# 为飞书准备表格内容
table_content = []

for i, item in enumerate(sorted_slow_logs[:50]):  # 只显示前50条
    db_name = item.get('DBName', 'N/A')
    
    # 获取SQL模板，注意这个API返回的是SQL模板
    sql_template = item.get('SQLText', 'N/A')
    if len(sql_template) > 300:  # 增加显示长度从100到300
        sql_template = sql_template[:297] + "..."
    
    # 统计数据
    total_count = item.get('MySQLTotalExecutionCounts', 0)
    total_time = float(item.get('MySQLTotalExecutionTimes', 0))
    avg_time = round(total_time / total_count if total_count > 0 else 0, 2)
    max_time = item.get('MaxExecutionTimeMS', 0)
    parse_rows = item.get('ParseTotalRowCounts', 0)
    max_scan_rows = item.get('ParseMaxRowCount', 0)
    
    # 添加到Markdown表格
    markdown_table += f"| {i+1} | {db_name} | `{sql_template}` | {total_count} | {avg_time} | {max_time} | {parse_rows} | {max_scan_rows} |\n"
    
    # 添加到飞书表格内容
    table_content.append([
        f"{i+1}",
        db_name,
        sql_template,
        str(total_count),
        str(avg_time),
        str(max_time),
        str(parse_rows),
        str(max_scan_rows)
    ])

print(markdown_table)

# === 推送到飞书群 ===
if len(all_slow_logs) > 0 and FEISHU_WEBHOOK and FEISHU_WEBHOOK != "YOUR_FEISHU_WEBHOOK_URL":
    # 使用卡片消息格式
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"🐢 慢 SQL 统计报告 ({start_time.strftime('%Y-%m-%d')} ~ {end_time.strftime('%Y-%m-%d')})"
                },
                "template": "orange"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**总共发现 {len(all_slow_logs)} 条慢查询统计记录，以下是执行次数最多的前20条:**"
                    }
                },
                {
                    "tag": "hr"
                }
            ]
        }
    }
    
    # 添加表格
    for i, item in enumerate(sorted_slow_logs[:20]):  # 只显示前20条
        db_name = item.get('DBName', 'N/A')
        sql_template = item.get('SQLText', 'N/A')
        if len(sql_template) > 500:  # 增加飞书消息中SQL显示长度从200到500
            sql_template = sql_template[:497] + "..."
        
        total_count = item.get('MySQLTotalExecutionCounts', 0)
        total_time = float(item.get('MySQLTotalExecutionTimes', 0))
        avg_time = round(total_time / total_count if total_count > 0 else 0, 2)
        max_time = item.get('MaxExecutionTimeMS', 0)
        parse_rows = item.get('ParseTotalRowCounts', 0)
        max_scan_rows = item.get('ParseMaxRowCount', 0)
        create_time = item.get('CreateTime', 'N/A')
        
        card["card"]["elements"].append({
            "tag": "div",
            "fields": [
                {
                    "is_short": False,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**#{i+1} SQL模板:** `{sql_template}`"
                    }
                }
            ]
        })
        
        card["card"]["elements"].append({
            "tag": "div",
            "fields": [
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**数据库:** {db_name}"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**创建时间:** {create_time}"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**执行次数:** {total_count}"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**平均执行时间:** {avg_time}ms"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**最大执行时间:** {max_time}ms"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**解析行数(总计):** {parse_rows}"
                    }
                }
            ]
        })
        
        # 添加分隔线
        if i < 19:  # 不在最后一条后添加分隔线
            card["card"]["elements"].append({
                "tag": "hr"
            })
    
    # 发送请求到飞书
    try:
        response = requests.post(
            FEISHU_WEBHOOK,
            headers={"Content-Type": "application/json"},
            data=json.dumps(card)
        )
        if response.status_code == 200:
            result = response.json()
            if result.get("code") == 0:
                print("[INFO] 成功发送慢SQL统计报告到飞书")
            else:
                print(f"[ERROR] 发送到飞书失败，错误码: {result.get('code')}, 消息: {result.get('msg')}")
        else:
            print(f"[ERROR] 发送到飞书失败，状态码: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] 发送到飞书时出错: {e}")
else:
    print("[INFO] 没有配置飞书 Webhook 或没有找到慢查询记录，跳过发送")

print("[INFO] 慢查询统计报告生成完成") 
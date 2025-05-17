# slow_sql_report.py

import json
import hashlib
import datetime
import requests
import sys
from collections import defaultdict
try:
    from aliyunsdkcore.client import AcsClient
    from aliyunsdkrds.request.v20140815.DescribeSlowLogRecordsRequest import DescribeSlowLogRecordsRequest
    from config import ACCESS_KEY_ID, ACCESS_KEY_SECRET, REGION_ID, DB_INSTANCE_ID, FEISHU_WEBHOOK
except ImportError as e:
    print(f"[ERROR] 导入依赖失败: {e}")
    sys.exit(1)

# === 显示配置信息（敏感信息部分隐藏）===
print(f"[DEBUG] 区域: {REGION_ID}")
print(f"[DEBUG] 实例ID: {DB_INSTANCE_ID}")
print(f"[DEBUG] ACCESS_KEY_ID: {ACCESS_KEY_ID[:4]}{'*' * (len(ACCESS_KEY_ID) - 8)}{ACCESS_KEY_ID[-4:]}")
print(f"[DEBUG] FEISHU_WEBHOOK: {'已配置' if FEISHU_WEBHOOK and FEISHU_WEBHOOK != 'YOUR_FEISHU_WEBHOOK_URL' else '未配置'}")

# === 获取上周时间范围 ===
end_time = datetime.datetime.now()
start_time = end_time - datetime.timedelta(days=7)
start_str = start_time.strftime("%Y-%m-%dT00:00Z")
end_str = end_time.strftime("%Y-%m-%dT00:00Z")

# === 初始化阿里云客户端 ===
try:
    client = AcsClient(ACCESS_KEY_ID, ACCESS_KEY_SECRET, REGION_ID)
    print("[INFO] 成功初始化阿里云客户端")
except Exception as e:
    print(f"[ERROR] 初始化阿里云客户端失败: {e}")
    sys.exit(1)

# === 构建请求 ===
request = DescribeSlowLogRecordsRequest()
request.set_DBInstanceId(DB_INSTANCE_ID)
request.set_StartTime(start_str)
request.set_EndTime(end_str)
request.set_accept_format('json')
# 设置每页记录数量
request.set_PageSize(100)

print(f"[INFO] 查询时间范围: {start_str} ~ {end_str}")

# === 发送请求并获取所有记录（分页处理） ===
all_slow_logs = []
page_number = 1
max_pages = 50  # 最多获取20页，对应5000条记录
total_records = 0

try:
    # 分页查询所有慢查询记录
    while page_number <= max_pages:
        request.set_PageNumber(page_number)
        response = client.do_action_with_exception(request)
        print(f"[INFO] 成功发送第{page_number}页请求至阿里云")
        result = json.loads(response)
        
        # 获取页面信息
        total_records = result.get('TotalRecordCount', 0)
        page_records = result.get('PageRecordCount', 0)
        
        print(f"[INFO] 总记录数: {total_records}, 当前页记录数: {page_records}, 当前页码: {page_number}")
        
        # 获取慢查询记录
        items = result.get("Items", {})
        page_slow_logs = items.get("SQLSlowRecord", [])
        
        if not page_slow_logs:
            print(f"[INFO] 第{page_number}页没有找到慢查询记录")
            break
        
        all_slow_logs.extend(page_slow_logs)
        print(f"[INFO] 已累计获取 {len(all_slow_logs)} 条慢查询记录")
        
        # 如果当前页记录数小于页大小，说明已经是最后一页
        if page_records < 100:
            break
            
        page_number += 1
    
    if not all_slow_logs:
        print("[WARN] 没有找到满足条件的慢查询记录")
        sys.exit(0)
    
    print(f"[INFO] 共获取到 {len(all_slow_logs)} 条慢查询记录，开始分析...")
    
except Exception as e:
    print(f"[ERROR] 调用阿里云API失败: {e}")
    sys.exit(1)

# === 数据聚合 ===
summary = defaultdict(lambda: {"count": 0, "total_time": 0.0, "max_time": 0.0, "total_scanned_rows": 0, "total_parse_rows": 0})

excluded_users = ["risk_dw_bin_ro"]  # 要排除的用户列表
excluded_count = 0

for record in all_slow_logs:
    sql = record.get("SQLText", "").strip()
    if not sql:
        continue
    
    # 获取用户名信息
    username = record.get("AccountName", "")
    # 排除指定用户的慢SQL
    if username in excluded_users:
        excluded_count += 1
        continue
        
    # 使用SQLHash作为键，这样更准确
    key = record.get("SQLHash", hashlib.md5(sql.encode()).hexdigest()[:10])
    
    # 查询时间，单位毫秒
    query_time = float(record.get("QueryTimeMS", 0))
    
    # 扫描行数 - 从ScanRows字段或新的字段获取
    scanned_rows = int(record.get("ScanRows", 0))
    if scanned_rows == 0:  # 如果ScanRows为0，尝试使用ReturnRowCounts
        scanned_rows = int(record.get("ReturnRowCounts", 0))
    
    # 解析行数 - 添加ParseRowCounts字段
    parse_rows = int(record.get("ParseRowCounts", 0))
    
    # 输出调试信息，帮助查看原始数据
    if page_number <= 1 and len(all_slow_logs) < 10:
        print(f"[DEBUG] SQL: {sql[:50]}...")
        print(f"[DEBUG] ScanRows: {record.get('ScanRows', 'N/A')}, ReturnRowCounts: {record.get('ReturnRowCounts', 'N/A')}, ParseRowCounts: {record.get('ParseRowCounts', 'N/A')}")
    
    # 更新或初始化记录
    if key not in summary:
        summary[key]["sql"] = sql  # 保存完整SQL，不再截断
        
    summary[key]["count"] += int(record.get("QueryTimes", 1))
    summary[key]["total_time"] += query_time
    summary[key]["db_name"] = record.get("DBName", "")
    summary[key]["max_time"] = max(summary[key]["max_time"], query_time)
    summary[key]["host_address"] = record.get("HostAddress", "")
    summary[key]["username"] = username  # 保存用户名信息
    summary[key]["total_scanned_rows"] += scanned_rows
    summary[key]["total_parse_rows"] += parse_rows

print(f"[INFO] 已排除 {excluded_count} 条来自 {', '.join(excluded_users)} 用户的记录")

# === 计算综合评分 ===
for key, data in summary.items():
    avg_time = data["total_time"] / data["count"]
    # 综合评分 = 平均执行时间 × 执行次数 × log(扫描行数+1)
    data["score"] = avg_time * data["count"] * max(1, (data["total_scanned_rows"] / data["count"]) ** 0.5 / 10)

# === 排序并生成 Markdown 表格 ===
top_slow_sql = sorted(summary.values(), key=lambda x: x["score"], reverse=True)[:200]
print(f"[INFO] 生成了 {len(top_slow_sql)} 条聚合的慢查询数据")

# 为飞书准备表格内容
table_content = []
for item in top_slow_sql:
    avg = round(item["total_time"] / item["count"], 2)
    max_time = round(item["max_time"], 2)
    avg_rows = round(item["total_scanned_rows"] / item["count"]) if item["count"] > 0 else 0
    avg_parse_rows = round(item["total_parse_rows"] / item["count"]) if item["count"] > 0 else 0
    table_content.append([
        item['sql'], 
        item['db_name'], 
        item['host_address'], 
        item.get('username', '未知'),
        str(item['count']), 
        str(avg), 
        str(max_time),
        str(avg_rows),
        str(avg_parse_rows)
    ])

# === 推送到飞书群 ===
if len(top_slow_sql) > 0 and FEISHU_WEBHOOK and FEISHU_WEBHOOK != "YOUR_FEISHU_WEBHOOK_URL":
    # 使用卡片消息格式，提高可读性
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": f"🐢 本周慢 SQL 报告（{start_time.date()} ~ {end_time.date()}）"
                },
                "template": "red"
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**总共发现 {total_records} 条慢查询记录，分析了 {len(all_slow_logs)} 条（排除了 {excluded_count} 条 {', '.join(excluded_users)} 用户的记录），以下是最需要优化的前200条:**"
                    }
                },
                {
                    "tag": "hr"
                }
            ]
        }
    }
    
    # 添加每条慢查询的详细信息（仅展示前20条详情，其余以表格形式展示）
    for i, item in enumerate(top_slow_sql[:20]):
        avg_time = round(item["total_time"] / item["count"], 2)
        max_time = round(item["max_time"], 2)
        # 确保正确解析行数
        avg_rows = round(item["total_scanned_rows"] / item["count"]) if item["count"] > 0 else 0
        avg_parse_rows = round(item["total_parse_rows"] / item["count"]) if item["count"] > 0 else 0
        
        # 截断过长的SQL，使消息更美观
        sql_display = item['sql']
        if len(sql_display) > 200:
            sql_display = sql_display[:197] + "..."
        
        # 美化显示
        card["card"]["elements"].append({
            "tag": "div",
            "fields": [
                {
                    "is_short": False,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**#{i+1} SQL:** `{sql_display}`"
                    }
                }
            ]
        })
        
        # 添加详细信息表格
        card["card"]["elements"].append({
            "tag": "div",
            "fields": [
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**数据库:** {item['db_name']}"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**主机:** {item['host_address']}"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**账号:** {item.get('username', '未知')}"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**执行次数:** {item['count']}"
                    }
                }
            ]
        })
        
        # 性能指标
        card["card"]["elements"].append({
            "tag": "div",
            "fields": [
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**平均耗时:** {avg_time}ms"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**最大耗时:** {max_time}ms"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**平均扫描行数:** {avg_rows}"
                    }
                },
                {
                    "is_short": True,
                    "text": {
                        "tag": "lark_md",
                        "content": f"**平均解析行数:** {avg_parse_rows}"
                    }
                }
            ]
        })
        
        # 添加分隔线
        if i < len(top_slow_sql[:20]) - 1:
            card["card"]["elements"].append({
                "tag": "hr"
            })
    
    # 如果有超过20条记录，将剩余记录以简洁表格形式添加
    if len(top_slow_sql) > 20:
        table_rows = []
        for i, item in enumerate(top_slow_sql[20:200], 21):
            avg_time = round(item["total_time"] / item["count"], 2)
            max_time = round(item["max_time"], 2)
            avg_rows = round(item["total_scanned_rows"] / item["count"]) if item["count"] > 0 else 0
            avg_parse_rows = round(item["total_parse_rows"] / item["count"]) if item["count"] > 0 else 0
            
            # 表格中SQL还是需要限制长度，否则会影响可读性
            sql_preview = item['sql']
            if len(sql_preview) > 80:
                sql_preview = sql_preview[:77] + "..."
            
            username = item.get('username', '未知')
            
            table_rows.append(f"| {i} | {sql_preview} | {item['db_name']} | {username} | {item['count']} | {avg_time} | {avg_rows} | {avg_parse_rows} |")
        
        table_header = "| 序号 | SQL | 数据库 | 账号 | 执行次数 | 平均耗时(ms) | 平均扫描行数 | 平均解析行数 |\n|------|-----|--------|------|---------|------------|------------|------------|\n"
        table_content = table_header + "\n".join(table_rows)
        
        card["card"]["elements"].append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "**剩余需优化的SQL查询:**"
            }
        })
        
        # 将表格分段发送，避免内容过长
        table_chunks = [table_rows[i:i+30] for i in range(0, len(table_rows), 30)]
        for chunk_idx, chunk in enumerate(table_chunks):
            chunk_content = table_header + "\n".join(chunk)
            card["card"]["elements"].append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**记录 {20+chunk_idx*30+1}-{min(20+(chunk_idx+1)*30, len(top_slow_sql))}:**\n{chunk_content}"
                }
            })
    
    try:
        print("[INFO] 发送卡片消息到飞书...")
        resp = requests.post(FEISHU_WEBHOOK, data=json.dumps(card), headers={"Content-Type": "application/json"})
        print(f"[INFO] 推送状态: {resp.status_code}, 返回: {resp.text}")
        
        # 如果卡片消息失败，尝试发送简单文本消息
        if resp.status_code != 200:
            print("[WARN] 卡片消息发送失败，尝试发送简单文本消息...")
            simple_payload = {
                "msg_type": "text",
                "content": {
                    "text": f"🐢 本周慢 SQL 报告（{start_time.date()} ~ {end_time.date()}）\n\n" +
                            f"总共发现 {total_records} 条慢查询记录，分析了 {len(all_slow_logs)} 条（排除了 {excluded_count} 条 {', '.join(excluded_users)} 用户的记录），以下是最需要优化的前20条:\n\n" +
                            "\n".join([
                                f"- **#{i+1}** SQL: {item['sql'][:150]}...\n" +
                                f"  数据库: {item['db_name']} | 主机: {item['host_address']} | 账号: {item.get('username', '未知')}\n" +
                                f"  执行: {item['count']}次 | 平均: {avg_time}ms | 最大: {max_time}ms\n" +
                                f"  扫描行: {avg_rows} | 解析行: {avg_parse_rows}"
                                for i, item in enumerate(top_slow_sql[:20])
                            ]) + 
                            f"\n\n注意：共发现 {len(top_slow_sql)} 条需要优化的SQL，此处仅展示前20条。"
                }
            }
            resp = requests.post(FEISHU_WEBHOOK, data=json.dumps(simple_payload), headers={"Content-Type": "application/json"})
            print(f"[INFO] 简单消息推送状态: {resp.status_code}, 返回: {resp.text}")
    except Exception as e:
        print(f"[ERROR] 推送到飞书失败: {e}")
else:
    if len(top_slow_sql) == 0:
        print("[INFO] 没有慢查询数据，跳过推送")
    elif FEISHU_WEBHOOK == "YOUR_FEISHU_WEBHOOK_URL":
        print("[WARN] 飞书 Webhook URL 未配置，跳过推送")
    elif not FEISHU_WEBHOOK:
        print("[WARN] 飞书 Webhook URL 为空，跳过推送")

# 输出结果到控制台
print("\n===== 慢查询报告 =====")
print(f"时间范围: {start_time.date()} ~ {end_time.date()}")
print(f"总记录数: {total_records}, 分析记录数: {len(all_slow_logs)}")
print(f"已排除 {excluded_count} 条来自 {', '.join(excluded_users)} 用户的记录")
print("Top 200 慢查询:")
print("| 序号 | SQL | 数据库 | 主机 | 账号 | 次数 | 平均耗时(ms) | 最大耗时(ms) | 平均扫描行数 |")
print("|------|-----|--------|------|------|------|--------------|--------------|------------|")
for i, item in enumerate(top_slow_sql):
    avg = round(item["total_time"] / item["count"], 2)
    max_time = round(item["max_time"], 2)
    avg_rows = round(item["total_scanned_rows"] / item["count"]) if item["count"] > 0 else 0
    
    # 控制台输出时SQL仍需要限制长度，以便打印
    sql_preview = item['sql']
    if len(sql_preview) > 100:
        sql_preview = sql_preview[:97] + "..."
        
    username = item.get('username', '未知')
    print(f"| {i+1} | {sql_preview} | {item['db_name']} | {item['host_address']} | {username} | {item['count']} | {avg} | {max_time} | {avg_rows} |")
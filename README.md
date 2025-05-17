# 慢SQL监控工具

这个工具用于监控阿里云RDS数据库的慢查询，并生成周报发送到飞书群。

## 功能特点

- 自动获取阿里云RDS慢查询日志
- 分析和聚合SQL查询
- 计算平均执行时间、扫描行数和解析行数
- 生成详细报告并发送到飞书群
- 支持定时自动运行

## 必要条件

- Python 3.6+
- 阿里云ECS实例
- 阿里云RDS实例的访问权限
- 飞书群机器人配置

## 配置说明

1. 创建`config.py`文件，添加以下配置（替换为您的实际值）：

```python
# 阿里云访问凭证
ACCESS_KEY_ID = "您的阿里云ACCESS_KEY_ID"
ACCESS_KEY_SECRET = "您的阿里云ACCESS_KEY_SECRET"

# 阿里云RDS实例信息
REGION_ID = "cn-hangzhou"  # 根据您的实例所在区域修改
DB_INSTANCE_ID = "rm-bp12345678"  # 您的RDS实例ID

# 飞书Webhook
FEISHU_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
```

## 在阿里云ECS上部署

1. 将所有文件上传到ECS实例
2. 安装必要的依赖：

```bash
pip3 install aliyun-python-sdk-core aliyun-python-sdk-rds requests
```

3. 设置定时任务：

```bash
chmod +x crontab_setup.sh
./crontab_setup.sh
```

执行以上脚本将：
- 创建日志目录
- 创建运行脚本
- 设置每周一早上8点自动运行的定时任务

## 手动运行

如果想手动运行脚本，可以执行：

```bash
python3 slow_sql_report.py
```

或者使用设置脚本创建的运行脚本：

```bash
./run_slow_sql_report.sh
```

## 日志管理

日志文件将保存在`logs`目录下，格式为`slow_sql_report_YYYYMMDD_HHMMSS.log`。系统默认保留最近10份日志文件。

## 自定义配置

如果需要自定义配置，可以修改：
- `excluded_users`：用于排除特定用户的慢查询（在`slow_sql_report.py`中）
- 时间范围：默认分析过去7天的数据（在`slow_sql_report.py`中的`start_time`和`end_time`变量）
- 定时任务时间：修改`crontab_setup.sh`中的`CRON_JOB`变量

## 常见问题

1. **权限问题**：确保使用的阿里云访问凭证具有RDS只读权限
2. **网络问题**：确保ECS实例能够访问阿里云API和飞书API
3. **时区问题**：默认使用系统时区，如需调整可修改时间相关代码 
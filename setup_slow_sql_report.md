# 慢SQL报告定时执行配置指南

## 文件准备

1. 确保已上传以下文件到ECS服务器：
   - `slow_sql_report.py` - 主脚本文件
   - `config.py` - 包含API密钥等配置信息

2. 创建配置文件(如果尚未创建)：
```python
# config.py
ACCESS_KEY_ID = "您的阿里云ACCESS_KEY_ID"
ACCESS_KEY_SECRET = "您的阿里云ACCESS_KEY_SECRET"
REGION_ID = "您的地区ID，如cn-hangzhou"
DB_INSTANCE_ID = "您的RDS实例ID"
FEISHU_WEBHOOK = "您的飞书Webhook地址"
```

## 安装依赖项

```bash
# 安装Python3和pip（如果尚未安装）
yum install -y python3 python3-pip   # CentOS/RHEL
# 或
apt-get install -y python3 python3-pip   # Debian/Ubuntu

# 创建虚拟环境（推荐）
mkdir -p /opt/scripts
cd /opt/scripts
python3 -m venv venv

# 激活虚拟环境
source /opt/scripts/venv/bin/activate

# 安装依赖项
pip install aliyun-python-sdk-core aliyun-python-sdk-rds requests pytz
```

## 设置脚本目录

```bash
# 创建脚本目录
mkdir -p /opt/scripts/slow_sql

# 复制文件（假设当前目录有这些文件）
cp slow_sql_report.py config.py /opt/scripts/slow_sql/

# 设置权限
chmod 750 /opt/scripts/slow_sql/slow_sql_report.py
chmod 640 /opt/scripts/slow_sql/config.py
```

## 设置定时任务

```bash
# 编辑crontab
crontab -e
```

添加以下行，设置每周一早上9点执行：

```
# 每周一上午9点执行慢SQL报告脚本
0 9 * * 1 cd /opt/scripts/slow_sql && /opt/scripts/venv/bin/python slow_sql_report.py >> /opt/scripts/slow_sql/report.log 2>&1
```

## 日志轮转设置

为避免日志文件过大，配置logrotate：

```bash
cat > /etc/logrotate.d/slow_sql_report << EOF
/opt/scripts/slow_sql/report.log {
    weekly
    rotate 12
    compress
    missingok
    notifempty
    create 0640 root root
}
EOF
```

## 测试脚本

```bash
# 激活虚拟环境
source /opt/scripts/venv/bin/activate

# 切换到脚本目录
cd /opt/scripts/slow_sql

# 手动执行一次，检查是否工作正常
python slow_sql_report.py
```

## 安全提示

1. 使用专门的RAM账户，仅授予必要的只读权限
2. 定期更换ACCESS_KEY，提高安全性
3. 使用加密存储ACCESS_KEY和其他敏感信息
4. 限制config.py文件访问权限

## 故障排查

如果遇到问题，请检查：

1. 时间格式是否符合阿里云API要求
2. 时区设置是否正确
3. 权限是否设置正确
4. 网络连接是否正常
5. 查看日志文件获取详细错误信息

## 监控脚本执行

可以添加一个简单的监控脚本，如果慢SQL报告未在预期时间执行，发送告警：

```bash
# 创建监控脚本 check_slow_sql_report.sh
cat > /opt/scripts/check_slow_sql_report.sh << EOF
#!/bin/bash
LOG_FILE="/opt/scripts/slow_sql/report.log"
CURRENT_DATE=\$(date +"%Y-%m-%d")
if ! grep -q "\$CURRENT_DATE" \$LOG_FILE && [ \$(date +"%u") -eq 1 ] && [ \$(date +"%H") -gt 10 ]; then
    echo "警告：今天的慢SQL报告似乎未执行" | mail -s "慢SQL报告执行失败" your-email@example.com
fi
EOF

chmod +x /opt/scripts/check_slow_sql_report.sh

# 添加到crontab，每周一11点检查
0 11 * * 1 /opt/scripts/check_slow_sql_report.sh
``` 
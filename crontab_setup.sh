#!/bin/bash

# 这个脚本帮助在阿里云ECS上设置定时任务，每周一运行slow_sql_report.py

# 显示当前目录
echo "当前目录: $(pwd)"

# 存储当前脚本绝对路径
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
echo "脚本目录: $SCRIPT_DIR"

# 确保安装了必要的依赖
echo "检查并安装依赖..."
pip3 install aliyun-python-sdk-core aliyun-python-sdk-rds requests

# 创建日志目录
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"
echo "日志将保存在: $LOG_DIR"

# 创建运行脚本
RUN_SCRIPT="$SCRIPT_DIR/run_slow_sql_report.sh"
cat > "$RUN_SCRIPT" << 'EOL'
#!/bin/bash

# 获取脚本所在目录的绝对路径
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

# 日志目录和文件
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/slow_sql_report_$(date +\%Y\%m\%d_\%H\%M\%S).log"

# 确保日志目录存在
mkdir -p "$LOG_DIR"

# 运行脚本并记录日志
echo "开始运行 slow_sql_report.py，日期: $(date)" > "$LOG_FILE"
cd "$SCRIPT_DIR" && python3 slow_sql_report.py >> "$LOG_FILE" 2>&1
echo "完成运行，日期: $(date)" >> "$LOG_FILE"

# 保留最近10个日志文件
cd "$LOG_DIR" && ls -t slow_sql_report_*.log | tail -n +11 | xargs -r rm
EOL

# 赋予执行权限
chmod +x "$RUN_SCRIPT"
echo "已创建运行脚本: $RUN_SCRIPT"

# 设置crontab定时任务（每周一早上8点运行）
CRON_JOB="0 10 * * 1 $RUN_SCRIPT"
(crontab -l 2>/dev/null | grep -v "$RUN_SCRIPT"; echo "$CRON_JOB") | crontab -
echo "已设置crontab任务，每周一早上10点运行"

# 显示当前crontab配置
echo "当前crontab配置:"
crontab -l

echo "设置完成！脚本将每周一早上10点自动运行，日志将保存在 $LOG_DIR 目录"
echo "您也可以通过运行 $RUN_SCRIPT 手动执行脚本" 
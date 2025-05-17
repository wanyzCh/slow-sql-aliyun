#!/bin/bash
#
# setup_dependencies.sh - 自动安装慢SQL报告脚本所需的依赖项
# 用法: bash setup_dependencies.sh [目标目录]
#

# 默认安装目录
INSTALL_DIR="${1:-/opt/scripts/slow_sql}"
VENV_DIR="/opt/scripts/venv"
CONFIG_TEMPLATE="config_template.py"
LOG_FILE="setup.log"

echo "开始安装慢SQL报告依赖... $(date)" | tee -a $LOG_FILE

# 检查是否为root用户
if [ "$(id -u)" != "0" ]; then
   echo "错误: 此脚本需要root权限" | tee -a $LOG_FILE
   echo "请使用 sudo 运行此脚本" | tee -a $LOG_FILE
   exit 1
fi

# 创建目录
mkdir -p $INSTALL_DIR
echo "创建目录: $INSTALL_DIR" | tee -a $LOG_FILE

# 检测系统类型
if [ -f /etc/debian_version ]; then
    echo "检测到 Debian/Ubuntu 系统" | tee -a $LOG_FILE
    apt-get update
    apt-get install -y python3 python3-pip python3-venv
elif [ -f /etc/redhat-release ]; then
    echo "检测到 CentOS/RHEL 系统" | tee -a $LOG_FILE
    yum install -y python3 python3-pip
else
    echo "未知系统类型，尝试通用安装方法" | tee -a $LOG_FILE
fi

# 创建虚拟环境
if [ ! -d "$VENV_DIR" ]; then
    echo "创建Python虚拟环境: $VENV_DIR" | tee -a $LOG_FILE
    python3 -m venv $VENV_DIR
    if [ $? -ne 0 ]; then
        echo "创建虚拟环境失败，尝试不使用venv模块" | tee -a $LOG_FILE
        pip3 install virtualenv
        virtualenv $VENV_DIR
    fi
else
    echo "虚拟环境已存在: $VENV_DIR" | tee -a $LOG_FILE
fi

# 激活虚拟环境并安装依赖
echo "安装Python依赖..." | tee -a $LOG_FILE
source $VENV_DIR/bin/activate
pip install --upgrade pip
pip install aliyun-python-sdk-core aliyun-python-sdk-rds requests

# 检查依赖安装状态
if pip list | grep -q "aliyun-python-sdk-rds"; then
    echo "依赖安装成功" | tee -a $LOG_FILE
else
    echo "警告: 依赖可能未正确安装，请检查错误信息" | tee -a $LOG_FILE
fi

# 创建配置文件模板
if [ ! -f "$INSTALL_DIR/config.py" ]; then
    echo "创建配置文件模板..." | tee -a $LOG_FILE
    cat > $INSTALL_DIR/config.py << EOF
# config.py
ACCESS_KEY_ID = "您的阿里云ACCESS_KEY_ID"
ACCESS_KEY_SECRET = "您的阿里云ACCESS_KEY_SECRET"
REGION_ID = "您的区域ID，如cn-hangzhou"
DB_INSTANCE_ID = "您的RDS实例ID"
FEISHU_WEBHOOK = "您的飞书Webhook地址"
EOF
    chmod 640 $INSTALL_DIR/config.py
    echo "配置模板已创建: $INSTALL_DIR/config.py" | tee -a $LOG_FILE
    echo "请编辑此文件并填入您的实际配置信息" | tee -a $LOG_FILE
else
    echo "配置文件已存在，跳过创建" | tee -a $LOG_FILE
fi

# 复制脚本文件（如果存在）
if [ -f "slow_sql_report.py" ]; then
    echo "复制脚本文件..." | tee -a $LOG_FILE
    cp slow_sql_report.py $INSTALL_DIR/
    chmod 750 $INSTALL_DIR/slow_sql_report.py
    echo "脚本已复制到: $INSTALL_DIR/slow_sql_report.py" | tee -a $LOG_FILE
else
    echo "未找到脚本文件，请手动复制slow_sql_report.py到 $INSTALL_DIR/" | tee -a $LOG_FILE
fi

# 设置crontab
echo "设置每周一早上9点自动执行的定时任务..." | tee -a $LOG_FILE
CRON_JOB="0 9 * * 1 cd $INSTALL_DIR && $VENV_DIR/bin/python slow_sql_report.py >> $INSTALL_DIR/report.log 2>&1"

# 检查crontab中是否已存在相同的任务
if crontab -l | grep -q "$INSTALL_DIR.*slow_sql_report.py"; then
    echo "定时任务已存在，跳过添加" | tee -a $LOG_FILE
else
    # 添加到crontab
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    echo "定时任务已添加" | tee -a $LOG_FILE
fi

# 配置logrotate
echo "配置日志轮转..." | tee -a $LOG_FILE
cat > /etc/logrotate.d/slow_sql_report << EOF
$INSTALL_DIR/report.log {
    weekly
    rotate 12
    compress
    missingok
    notifempty
    create 0640 root root
}
EOF
echo "日志轮转已配置" | tee -a $LOG_FILE

echo "安装完成！" | tee -a $LOG_FILE
echo "您可以使用以下命令测试脚本:" | tee -a $LOG_FILE
echo "  source $VENV_DIR/bin/activate" | tee -a $LOG_FILE
echo "  cd $INSTALL_DIR" | tee -a $LOG_FILE
echo "  python slow_sql_report.py" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "请确保先编辑 $INSTALL_DIR/config.py 填入您的配置信息" | tee -a $LOG_FILE 
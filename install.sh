#!/bin/bash 

# 获取当前脚本所在绝对路径 
cur_dir=$(cd "$(dirname "$0")"; pwd) 

# 拷贝配置文件
cp -r es_data_remover /etc/ 

# 设置cron任务
echo "please enter cron time(12:58 daily eg.: 12 58): " 
read hour min 
echo $min" "$hour" * * * root python "$cur_dir"/run.py >> /var/log/es_data_remover.log" >> /etc/crontab  

# 启动cron
#service cron start  
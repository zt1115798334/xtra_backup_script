# -*- coding: utf-8 -*-
import datetime
import logging
import os
import shutil
import socket
import sys
import time

logging.basicConfig(level=logging.INFO
                    # handlers={logging.FileHandler(filename='backup_log_info.log', mode='a', encoding='utf-8')}
                    )

# host = "mysql-server"
# port = "3306"
# user = "root"
# password = "management"
# backup_dir = "/data"
# backup_file_list = os.path.join(backup_dir, "backup_file_list.log")
# backup_keep_days = 15

host = os.getenv("px_host")
port = os.getenv("px_port")
user = os.getenv("px_user")
password = os.getenv("px_password")
backup_dir = os.getenv("px_dir")
backup_file_list = os.path.join(backup_dir, "backup_file_list.log")
backup_keep_days = os.getenv("px_keep_days")


# 获取备份类型，周六进行完备，平时增量备份，如果没有全备，执行完整备份
def get_backup_type():
    if os.path.exists(backup_file_list):
        with open(backup_file_list, 'r') as f:
            lines = f.readlines()
            if lines:
                last_line = lines[-1]  # get last backup name
                if last_line:
                    print(time.localtime().tm_wday)
                    if time.localtime().tm_wday == 1:
                        backupType = "full"
                    else:
                        backupType = "incr"
                else:
                    backupType = "full"
            else:
                backupType = "full"
    else:
        # full backup when first backup
        open(backup_file_list, "a").close()
        backupType = "full"
    return backupType


# 获取最后一次备份信息
def get_last_backup():
    last_backup = None
    if os.path.exists(backup_file_list):
        with open(backup_file_list, 'r') as f:
            lines = f.readlines()
            last_line = lines[-1]  # get last backup name
            if last_line:
                last_backup = os.path.join(backup_dir, last_line.split("|")[-1])
    return last_backup.replace("\n", "")


# 探测实例端口号
def get_mysql_service_status():
    mysqlStat = 0
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = s.connect_ex((host, int(port)))
    # port os open
    if result == 0:
        mysqlStat = 1
    return mysqlStat


# 清理过期的历史备份信息
def clean_expired_file():
    for backup_name in os.listdir(backup_dir):
        bn = "{0}/{1}".format(backup_dir, backup_name)
        if os.path.isdir(bn):
            bak_datetime = datetime.datetime.strptime(backup_name.replace("_full", "").replace("_incr", ""),
                                                      '%Y%m%d_%H%M%S')
            if bak_datetime < datetime.datetime.now() - datetime.timedelta(days=backup_keep_days):
                shutil.rmtree(os.path.join(backup_dir, backup_name))


# 完整备份
def full_backup(backupFileName):
    os.system("[ ! -d {0}/{1} ] && mkdir -p {0}/{1}".format(backup_dir, backupFileName))
    logfile = os.path.join(backup_dir, "{0}/{1}/backup-log.log".format(backup_dir, backupFileName))
    backup_cmd = '''xtrabackup --backup --user={0} --password={1} --host={2} --port={3} \
    --stream=xbstream --compress --compress-threads=8 --parallel=4 \
    --extra-lsndir={4}/{5}  > {4}/{5}/{5}.xbstream 2>{6}'''.format(
        user, password, host, port,
        backup_dir, backupFileName, logfile)

    # backup_cmd = '''xtrabackup --backup --user={0} --password={1} --host={2} --port={3} \
    # --target-dir={4}/{5}'''.format(
    #     user, password, host, port,
    #     backup_dir, backupFileName, logfile)
    print(backup_cmd)
    return os.system(backup_cmd)


# 增量备份
def incr_backup(backupFileName):
    os.system("[ ! -d {0}/{1} ] && mkdir -p {0}/{1}".format(backup_dir, backupFileName))
    logfile = os.path.join(backup_dir, "{0}/{1}/backup-log.log".format(backup_dir, backupFileName))
    # 增量备份基于上一个增量/完整备份
    incremental_basedir = get_last_backup()
    backup_cmd = '''xtrabackup --backup --user={0} --password={1} --host={2} --port={3} \
    --stream=xbstream --compress --compress-threads=8 --parallel=4 \
    --incremental-basedir={7} \
    --extra-lsndir={4}/{5}  > {4}/{5}/{5}.xbstream 2>{6}'''.format(
        user, password, host, port,
        backup_dir, backupFileName, logfile, incremental_basedir)

    # backup_cmd = '''xtrabackup --backup --user={0} --password={1} --host={2} --port={3} \
    # --incremental-basedir={7} --target-dir={4}/{5}'''.format(
    #     user, password, host, port,
    #     backup_dir, backupFileName, logfile, incremental_basedir)
    print(backup_cmd)
    return os.system(backup_cmd)


if __name__ == '__main__':
    print("host:{0}, port:{1}, user:{2}, password:{3}, backup_dir:{4}, backup_keep_days:{5}"
          .format(host, port, user, password, backup_dir, backup_keep_days))
    mysql_stat = get_mysql_service_status()
    backup_type = get_backup_type()
    print(backup_type)
    if mysql_stat <= 0:
        logging.info("mysql instance is inactive,backup exit")
        sys.exit(1)
    try:
        start_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        logging.info(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "--------start backup")
        backup_file_name = start_time
        execute_result = None
        if backup_type == "full":
            backup_file_name = backup_file_name + "_full"
            logging.info("execute full backup......")
            execute_result = full_backup(backup_file_name)
            if execute_result == 0:
                logging.info(
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "--------begin cleanup history backup")
                logging.info("execute cleanup backup history......")
                clean_expired_file()
                logging.info(
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "--------finish cleanup history backup")
        else:
            backup_file_name = backup_file_name + "_incr"
            logging.info("execute incr backup......")
            execute_result = incr_backup(backup_file_name)
        if execute_result == 0:
            finish_time = datetime.datetime.now().strftime('%Y%m%d_H%M%S')
            backup_info = start_time + "|" + finish_time + "|" + start_time + "_" + backup_type
            with open(backup_file_list, 'a+') as f:
                f.write(backup_info + '\n')
            logging.info(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "--------finish backup")
        else:
            logging.info(
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "--------xtrabackup failed.please check log")
    except:
        raise
        sys.exit(1)

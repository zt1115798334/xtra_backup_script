# -*- coding: utf-8 -*-
import datetime
import logging
import os
import time

logging.basicConfig(level=logging.INFO
                    # handlers={logging.FileHandler(filename='restore_log_info.log', mode='a', encoding='utf-8')}
                    )

host = "mysql-server"
port = "3306"
user = "root"
password = "management"
instance_name = "mysqld_7000"
stop_at = "2019-08-01 18:50:59"
cnf_file = "/usr/local/mysql57_data/mysql7000/etc/my.cnf"
backup_dir = "/data"
dest_dir = "/temp/restore_tmp/"
xtra_backup_log_name = "backup_log.log"
backup_file_list = os.path.join(backup_dir, "backup_file_list.log")


# 根据key值，获取MySQL配置文件中的value
def get_config_value(key):
    value = None
    if not key:
        return value
    if os.path.exists(cnf_file):
        with open(cnf_file, 'r') as f:
            for line in f:
                if (line.split("=")[0]):
                    if (line[0:1] != "#" and line[0:1] != "["):
                        if (key == line.split("=")[0].strip()):
                            value = line.split("=")[1].strip()
    return value


def stop_mysql_service():
    print("################stop mysql service###################")
    print("systemctl stop {}".format(instance_name))


def start_mysql_service():
    print("################stop mysql service###################")
    print("systemctl start {0}".format(instance_name))


# 返回备份日志中的最新的一个早于stop_at时间的完整备份，以及其后面的增量备份
def get_restore_file_list():
    list_backup = []
    list_restore_file = []
    if os.path.exists(backup_file_list):
        with open(backup_file_list, 'r') as f:
            lines = f.readlines()
            for line in lines:
                list_backup.append(line.replace("\n", ""))
    if list_backup:
        for i in range(len(list_backup) - 1, -1, -1):
            list_restore_file.append(list_backup[i])
            backup_name = list_backup[i].split("|")[2]
            if "full" in backup_name:
                full_backup_time = list_backup[i].split("|")[1]
                if stop_at < full_backup_time:
                    break
                else:
                    list_restore_file = None
    # restore file in the list_restore_log
    list_restore_file.reverse()
    return list_restore_file


# 解压缩需要还原的备份文件，包括一个完整备份以及N个增量备份（N>=0）
def uncompress_backup_file():
    print("################uncompress backup file###################")
    list_restore_backup = get_restore_file_list()

    # 如果没有生成时间早于stop_at的完整备份，无法恢复，退出
    if not list_restore_backup:
        raise "There is no backup that can be restored"
        exit(1)

    for restore_log in list_restore_backup:
        # 解压备份文件
        backup_name = restore_log.split("|")[2]
        backup_path = restore_log.split("|")[2]
        backup_full_name = os.path.join(backup_dir, backup_path, backup_name)
        backup_path = os.path.join(backup_dir, restore_log.split("|")[-1])
        # print('''[ ! -d {0} ] && mkdir -p {0}'''.format(os.path.join(dest_dir,backup_name)))
        os.system('''[ ! -d {0} ] && mkdir -p {0}'''.format(os.path.join(dest_dir, backup_name)))
        # print("xbstream -x < {0}.xbstream -C {1}".format(backup_full_name,os.path.join(dest_dir,backup_name)))
        os.system("xbstream -x < {0}.xbstream -C {1}".format(backup_full_name, os.path.join(dest_dir, backup_name)))
        # print("cd {0}".format(os.path.join(dest_dir,backup_name)))
        os.system("cd {0}".format(os.path.join(dest_dir, backup_name)))
        # print('''for f in `find {0}/ -iname "*\.qp"`; do qpress -dT4 $f  $(dirname $f) && rm -f $f; done '''.format(os.path.join(dest_dir,backup_name)))
        os.system('''for f in `find {0}/ -iname "*\.qp"`; do qpress -dT4 $f  $(dirname $f) && rm -f $f; done'''.format(
            os.path.join(dest_dir, backup_name)))

        current_backup_begin_time = None
        current_backup_end_time = None
        # 比较当前备份的结束时间和stop_at,如果当前备份开始时间小于stop_at并且结束时间大于stop_at，解压缩备份结束
        with open(os.path.join(dest_dir, backup_name, "xtrabackup_info"), 'r') as f:
            for line in f:
                if line and line.split("=")[0].strip() == "start_time":
                    current_backup_begin_time = line.split("=")[1].strip()
                if line and line.split("=")[0].strip() == "end_time":
                    current_backup_end_time = line.split("=")[1].strip()
        # 按照stop_at时间点还原的最后一个数据库备份,结束从第一个完整备份开始的解压过程
        if current_backup_begin_time <= stop_at <= current_backup_end_time:
            break

    # 返回最后一个备份文件，需要备份文件中的xtrabackup_info，解析出当前备份的end_time，从而确认需要哪些binlog
    return backup_name


# 根据返回最后一个备份文件，需要备份文件中的xtrabackup_info，结合stop_at，确认需要还原的binlog文件，以及binlog的position信息
def restore_database_binlog(last_backup_file):
    print("################restore data from binlog###################")
    binlog_dir = get_config_value("log-bin")
    if not (backup_dir):
        binlog_dir = get_config_value("log_bin")
    print("cd {0}".format(os.path.dirname(binlog_dir)))

    last_backup_file = os.path.join(dest_dir, last_backup_file, "xtrabackup_info")
    # parse backuplog.log and get binlog name and position

    backup_position_binlog_file = None
    backup_position = None
    with open(last_backup_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if "binlog_pos = filename " in line:
                backup_position_binlog_file = line.replace("binlog_pos = filename ", "").split(",")[0]
                backup_position_binlog_file = backup_position_binlog_file.replace("'", "")
                backup_position = line.replace("binlog_pos = filename ", "").split(",")[1].strip()
                backup_position = backup_position.split(" ")[1].replace("'", "")
                pass
            else:
                continue
        # /usr/local/mysql57_data/mysql8000/log/bin_log/mysql_bin_1300
        binlog_config = get_config_value("log-bin")
        binlog_path = os.path.dirname(binlog_config)
        binlog_files = os.listdir(binlog_path)

        # 如果没有找到binlog，忽略binlog的还原
        if not binlog_files:
            exit(1)

        # 对binlog文件排序，按顺序遍历binlog，获取binlog的最后的修改时间，与stop_at做对比，判断还原的过程是否需要某个binlogfile
        binlog_files.sort()

        binlog_files_for_restore = []
        # 恢复数据库的指定时间点
        stop_at_time = datetime.datetime.strptime(stop_at, '%Y-%m-%d %H:%M:%S')
        for binlog in binlog_files:
            if (".index" in binlog or "relay" in binlog):
                continue

            # 保留最后一个备份中的binlog，以及其后面的binlog，这部分binlog会在还原的时候用到
            if (int(binlog.split(".")[-1]) >= int(backup_position_binlog_file.split(".")[-1])):
                binlog_files_for_restore.append(binlog)

        binlog_file_count = 0
        # 第一个文件，从上最后一个差异备份的position位置开始，最后一个文件，需要stop_at到指定的时间
        for binlog in binlog_files_for_restore:
            if not os.path.isdir(binlog):
                # binlog物理文件的最后修改时间
                binlog_file_updatetime = datetime.datetime.strptime(
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.stat(binlog_path + "/" + binlog).st_mtime)),
                    '%Y-%m-%d %H:%M:%S')
                # 判断binlog的生成时间，是否大于stop_at，对于修改时间大于stop_at的日志，需要全部还原，不需要stop_at指定截止点
                if stop_at_time > binlog_file_updatetime:
                    if (binlog_file_count < 1):
                        if (len(binlog_files_for_restore) == 1):
                            # 找到差异备份之后的第一个binlog，需要根据差异备份的position，来过来第一个binlog文件
                            restore_commond = '''mysqlbinlog {0}  --skip-gtids=true --start-position={1}  --stop-datetime="{2}" | mysql mysql -h{3} -u{4} -p{5} -P{6}''' \
                                .format(binlog, backup_position, stop_at, host, user, password, port)
                            print(restore_commond)
                            binlog_file_count = binlog_file_count + 1
                        else:
                            # 找到差异备份之后的第一个binlog，需要根据差异备份的position，来过来第一个binlog文件
                            restore_commond = '''mysqlbinlog {0}  --skip-gtids=true --start-position={1} | mysql mysql -h{2} -u{3} -p{4} -P{5}''' \
                                .format(binlog, backup_position, host, user, password, port)
                            print(restore_commond)
                            binlog_file_count = binlog_file_count + 1
                    else:
                        # 从第二个文件开始，binlog需要全部还原
                        restore_commond = '''mysqlbinlog {0}  --skip-gtids=true  | mysql mysql -h{1} -u{2} -p{3} -P{4}''' \
                            .format(binlog, host, user, password, port)
                        print(restore_commond)
                        binlog_file_count = binlog_file_count + 1
                else:
                    if (binlog_file_count < 1):
                        restore_commond = '''mysqlbinlog {0}  --skip-gtids=true --start-position={1} --stop-datetime={2} | mysql -h{3} -u{4} -p{5} -P{6}'''.format(
                            binlog, backup_position, stop_at, host, user, password, port)
                        print(restore_commond)
                        binlog_file_count = binlog_file_count + 1
                    else:
                        if (binlog_file_count >= 1):
                            restore_commond = '''mysqlbinlog {0}  --skip-gtids=true --stop-datetime="{1}" | mysql -h{2} -u{3} -p{4} -P{5}'''.format(
                                binlog, stop_at, host, user, password, port)
                            print(restore_commond)
                            binlog_file_count = binlog_file_count + 1
                            break


def apply_log_for_backup():
    list_restore_backup = get_restore_file_list()
    start_flag = 1
    full_backup_path = None

    for current_backup_file in list_restore_backup:
        # 解压备份文件
        current_backup_name = current_backup_file.split("|")[2]
        current_backup_fullname = os.path.join(dest_dir, current_backup_name)
        if (start_flag == 1):
            full_backup_path = current_backup_fullname
            start_flag = 0
            print("innobackupex --apply-log --redo-only {0}".format(full_backup_path))
        else:
            print("innobackupex --apply-log --redo-only {0} --incremental-dir={1}".format(full_backup_path,
                                                                                          current_backup_fullname))
    # apply_log for full backup at last(remove --read-only parameter)
    print("innobackupex --apply-log {0}".format(full_backup_path))


def restore_backup_data():
    print("####################backup current database file###########################")
    datadir_path = get_config_value("datadir")
    print("mv {0} {1}".format(datadir_path, datadir_path + "_" + datetime.datetime.now().strftime('%Y%m%d%H%M%S')))
    print("mkdir {0}".format(datadir_path))
    print("chown -R mysql.mysql {0}".format(datadir_path))
    print("################restore backup data###################")
    list_restore_backup = get_restore_file_list()
    full_restore_path = dest_dir + list_restore_backup[0].split("|")[-1].replace(".xbstream", "")
    print("innobackupex --defaults-file={0} --copy-back --rsync {1}".format(cnf_file, full_restore_path))
    print("chown -R mysql.mysql {0}".format(datadir_path))


def restore_database():
    # 解压缩需要还原的备份文件
    last_backup_file_path = uncompress_backup_file()
    # 对备份文件apply-log
    apply_log_for_backup()
    # 停止mysql服务
    stop_mysql_service()
    # 恢复备份
    restore_backup_data()
    # 启动MySQL服务
    start_mysql_service()
    # 从binlog中恢复数据
    restore_database_binlog(last_backup_file_path)


if __name__ == '__main__':
    restore_database()

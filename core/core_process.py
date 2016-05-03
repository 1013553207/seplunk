#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import subprocess
from multiprocessing import Process, Pipe
from xml.etree import ElementTree as ET

import pyhdfs

from core import sql
from decompiler import process_jar
from core.logger import get_log
from core.configure import get_configure
from core.configure import get_extension
from core.configure import get_property

LOGGER = get_log(__file__)

TMP_PATH = "/tmp/hadoop"
DB_FLIE = "mydb.db"

CREATE_USER = False
CREATE_JOB_SUMMARY = False
CREATE_JOB_CONF = False

STAGE = ".staging"
USER_TABLE = 'user'
HEALTH_POINT = 4
STATUS_RUNNING = "RUNNING"

def save_user(conn, username, forbid):
    global CREATE_USER, USER_TABLE
    create_sql = ''' CREATE TABLE IF NOT EXISTS '%s'(
                'uid' INTEGER PRIMARY KEY AUTOINCREMENT,
                'username' VARCHAR(255) NOT NULL UNIQUE,
                'forbid' TINYINT(8) NOT NULL DEFAULT 0,
                'health_point' INTEGER NOT NULL DEFAULT %d)''' % (USER_TABLE, HEALTH_POINT)
    if not CREATE_USER:
        sql.create_table(conn, create_sql)
        CREATE_USER = True
    select_sql = "SELECT * FROM %s WHERE %s = '%s' " % (USER_TABLE, "username", username)
    LOGGER.info(select_sql)
    result = sql.fetchone(conn, select_sql)
    if result and forbid == 1:
        update_sql = "UPDATE %s SET forbid = '%s' WHERE username = '%s'"  % (USER_TABLE, forbid, username)
        sql.execute(conn, update_sql)
    if not result:
        insert_sql = "INSERT INTO '%s'(%s, %s) values('%s', '%s')" % (USER_TABLE, 'username', 'forbid', username, forbid)
        LOGGER.info(insert_sql)
        sql.execute(conn, insert_sql)


def save_job_summary_file(hdfs_client, conn, log_fullpath, f):
    global CREATE_JOB_SUMMARY, USER_TABLE, STATUS_RUNNING
    jobid = f[:len(f)-8]
    summary_file = os.path.join(log_fullpath, f)
    lines = ""
    for line in hdfs_client.open(summary_file):
        lines += line
    job_info = dict((pair.split("=")) for pair in lines.split(","))
    job_info['job_id'] = jobid
    sql_table = "job_summary"
    if not CREATE_JOB_SUMMARY:
        job_info['job_checksum'] = 0
        job_info['failedMaps'] = 0
        job_info['failedReduces'] = 0
        create_sql = '''CREATE TABLE IF NOT EXISTS '%s' (''' % sql_table
        for key in job_info.keys():
            if 'jobId' not in key:
                create_sql += "'%s' varchar(255)," % str(key)
        create_sql += '''CONSTRAINT 'job_summaryId_pri' PRIMARY KEY('user', 'job_id'),
                        CONSTRAINT 'job_summaryId_ref' FOREIGN KEY ('user') REFERENCES %s('username')
                    )''' % USER_TABLE
        LOGGER.info("SQL:" + create_sql)
        sql.create_table(conn, create_sql)
        CREATE_JOB_SUMMARY = True

    select_sql = "SELECT * FROM %s WHERE job_id = '%s'" % (sql_table, jobid)
    LOGGER.info(select_sql)
    result = sql.fetchone(conn, select_sql)
    if result:
        update = ", ".join([ "%s='%s'" % (key, job_info[key]) for key in job_info.keys() if 'jobId' not in key])
        update_sql = "UPDATE %s SET %s WHERE job_id = '%s' " % (sql_table, update, jobid)
        LOGGER.info(update_sql)
        sql.execute(conn, update_sql)
    else:
        keys, values = "", ""
        items = job_info.items()
        # keys = ", ".join(map(lambda item: "'%s'" % item[0], items))
        # values = ", ".join(map(lambda item: "'%s'" % item[1], items))
        keys = ", ".join(["%s" % key for key in job_info.keys() if 'jobId' not in key])
        values = ", ".join(["'%s'" % job_info[key] for key in job_info.keys() if 'jobId' not in key])
        sql_insert =  ("INSERT INTO %s(%s) VALUES (%s)") % (sql_table, keys, values)
        LOGGER.info(sql_insert)
        sql.execute(conn, sql_insert)


def save_job_summary(conn, job_info):
    sql_table = "job_summary"
    jobid = job_info['job_id']
    select_sql = "SELECT * FROM %s WHERE job_id = '%s'" % (sql_table, jobid)
    LOGGER.info(select_sql)
    result = sql.fetchone(conn, select_sql)
    if result:
        update = ", ".join([ "%s='%s'" % (key, job_info[key]) for key in job_info.keys() if 'jobId' not in key])
        update_sql = "UPDATE %s SET %s WHERE job_id = '%s' " % (sql_table, update, jobid)
        LOGGER.info(update_sql)
        sql.execute(conn, update_sql)
    else:
        keys, values = "", ""
        items = job_info.items()
        # keys = ", ".join(map(lambda item: "'%s'" % item[0], items))
        # values = ", ".join(map(lambda item: "'%s'" % item[1], items))
        keys = ", ".join(["%s" % key for key in job_info.keys() if 'jobId' not in key])
        values = ", ".join(["'%s'" % job_info[key] for key in job_info.keys() if 'jobId' not in key])
        sql_insert =  ("INSERT INTO %s(%s) VALUES (%s)") % (sql_table, keys, values)
        LOGGER.info(sql_insert)
        sql.execute(conn, sql_insert)


def save_job_conf_file(hdfs_client, conn, log_fullpath, f, jobid):
    global CREATE_JOB_CONF
    conf_file = os.path.join(log_fullpath, f)
    propertys = get_property(hdfs_client.open(conf_file))
    propertys["job_id"] = jobid
    sql_table = "job_conf"
    if not CREATE_JOB_CONF:
        create_sql = "CREATE TABLE IF NOT EXISTS '%s' (" % sql_table
        for key in propertys.keys():
                create_sql += "'%s' varchar(255)," % key
        create_sql += "CONSTRAINT job_confId PRIMARY KEY ('job_id'))"
        LOGGER.info(create_sql)
        sql.create_table(conn, create_sql)
        CREATE_JOB_CONF = True
    select_sql = "SELECT * FROM %s WHERE job_id = '%s'" % (sql_table, jobid)
    LOGGER.info(select_sql)
    result = sql.fetchone(conn, select_sql)
    if result:
        update = ", ".join([ "'%s'='%s'" % (key, propertys[key]) for key in propertys.keys() if key != 'jobId'])
        update_sql = "UPDATE %s SET %s WHERE job_id = '%s' " % (sql_table, update, jobid)
        LOGGER.info(update_sql)
        sql.execute(conn, update_sql)
    else:
        keys, values = "", ""
        items = propertys.items()
        keys = ", ".join(map(lambda item: "'%s'" % item[0], items))
        values = ", ".join(map(lambda item: "'%s'" % item[1], items))
        sql_insert =  ("INSERT INTO %s(%s) VALUES (%s)") % (sql_table, keys, values)
        LOGGER.info(sql_insert)
        sql.execute(conn, sql_insert)

def save_job_conf(conn, propertys):
    sql_table = "job_conf"
    jobid = propertys['job_id']
    select_sql = "SELECT * FROM %s WHERE job_id = '%s'" % (sql_table, jobid)
    LOGGER.info(select_sql)
    result = sql.fetchone(conn, select_sql)
    if result:
        update = ", ".join([ "'%s'='%s'" % (key, propertys[key]) for key in propertys.keys()])
        update_sql = "UPDATE %s SET %s WHERE job_id = '%s' " % (sql_table, update, jobid)
        LOGGER.info(update_sql)
        sql.execute(conn, update_sql)
    else:
        keys, values = "", ""
        items = propertys.items()
        keys = ", ".join(map(lambda item: "'%s'" % item[0], items))
        values = ", ".join(map(lambda item: "'%s'" % item[1], items))
        sql_insert =  ("INSERT INTO %s(%s) VALUES (%s)") % (sql_table, keys, values)
        LOGGER.info(sql_insert)
        sql.execute(conn, sql_insert)


def parse_log(hdfs_client, config, conn):
    '''process log and save useful information in dbbase'''
    log_fullpath = config.get("mapreduce.jobhistory.intermediate-done-dir", None)
    if not log_fullpath or hdfs_client.exists(log_fullpath):
        LOGGER.error("intermediate-done-dir path not exists")
    users = hdfs_client.listdir(log_fullpath)
    for user in users:
        save_user(conn, user, 0)
        log_fullpath = os.path.join(log_fullpath, user)
        for f in hdfs_client.listdir(log_fullpath):
            if get_extension(f) == ".summary":
                save_job_summary_file(hdfs_client, conn, log_fullpath, f)
            elif get_extension(f) == ".xml":
                jobid = f[:len(f)-9]
                save_job_conf_file(hdfs_client, conn, log_fullpath, f, jobid)
            else:
                pass


def monitor_am(hdfs_client, config, conn):
    '''monitor and process running mapreducess task'''
    global STAGE, STATUS_RUNNING
    am_fullpath = config.get("yarn.app.mapreduce.am.staging-dir", None)
    LOGGER.info(am_fullpath)
    if not am_fullpath or not hdfs_client.exists(am_fullpath):
        LOGGER.error("intermediate-done-dir path not exists")
        sys.exit(1)
    users = hdfs_client.listdir(am_fullpath)
    for user in users:
        save_user(conn, user, 0)
        am_fullpath = os.path.join(am_fullpath, user)
        staging_path = os.path.join(am_fullpath, STAGE)
        if not hdfs_client.exists(staging_path):
            LOGGER.info(staging_path+"not exists")
        for jobid in hdfs_client.listdir(staging_path):
            flag = 0
            LOGGER.info("user: %s jobid: %s  is running" % (user, jobid))
            running_job_path = os.path.join(staging_path, jobid)
            for f in hdfs_client.listdir(running_job_path):
                extension = get_extension(f)
                fullpath = os.path.join(running_job_path, f)
                if extension == ".jar":
                    hdfs_client.copy_to_local(fullpath, os.path.join(TMP_PATH, f))
                    info = {}
                    jar_checksum, job_conf = process_jar(os.path.join(TMP_PATH, f))
                    info['job_checksum'] = jar_checksum
                    info['job_id'] = jobid
                    info['status'] = STATUS_RUNNING
                    info['user'] = user
                    save_job_summary(conn, info)
                    flag += 1
                    if job_conf:
                        LOGGER.info(job_conf)
                        job_conf['job_id'] = jobid
                        save_job_conf(conn, job_conf)
                elif extension == ".xml":
                    if f == "job.xml":
                        pass
                    elif jobid in f:
                        try:
                            save_job_conf_file(hdfs_client, conn, running_job_path, f, jobid)
                        except Exception, e:
                            LOGGER.info(str(e))
                        finally:
                            flag += 1
                    else:
                        pass
                else:
                    pass
            if flag == 2:
                admin_action(conn, user, jobid)

def admin_action(conn, user, job_id):
    global USER_TABLE
    select_sql = "select forbid from %s where username='%s'" % (USER_TABLE, user)
    forbid = int(sql.fetchone(conn, select_sql)[0])
    print forbid
    if forbid == 1:
        kill_job(job_id)
        return
    select_sql = "select job_id, job_checksum, status from job_summary where user = '%s'" % (user)
    results = sql.fetchall(conn, select_sql)
    if not results:
        LOGGER.info("the user: %s have not any task" % user)
        return
    select_sql = "select job_checksum from job_summary where user = '%s' and job_id = '%s'" % (user, job_id)
    checksum = sql.fetchone(conn, select_sql)[0]
    select_sql = "select * from job_conf where job_id = '%s'" % job_id
    conf = sql.fetchone(conn, select_sql)
    count  = 0
    for jobid, job_checksum, job_status in results:
        # print jobid, job_checksum, job_status, checksum
        if checksum == job_checksum and job_id != jobid:
            select_sql = "select * from job_conf where job_id = '%s'" % jobid
            job_conf = sql.fetchone(conn, select_sql)
            if 'FAILED' in job_status:
                count += 1
                if count == HEALTH_POINT:
                    save_user(conn, user, 1)
                    kill_job(job_id)


def kill_job(job_id):
    cmdline = "mapred job -kill %s" % job_id
    print cmdline
    LOGGER.info('exec'+cmdline)
    child = subprocess.Popen(cmdline, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    content = ""
    for line in child.stdout.readlines():
        content += line
    for line in child.stdout.readlines():
        content += line
    LOGGER.info(content)


def seplunk_start(config_path):
    if not os.path.exists(config_path):
        LOGGER.error("config file path not exists")
        sys.exit(1)
    config = get_configure(config_path)
    default_fs = config.get('fs.defaultFS', None)
    if not default_fs:
        LOGGER.error("hdfs not found")
    host = default_fs.split(':')[1].strip("/")
    hdfs_client = pyhdfs.HdfsClient(host)
    if not os.path.exists(TMP_PATH):
        os.mkdir(TMP_PATH)
    db_path = os.path.join(TMP_PATH, DB_FLIE)
    # if os.path.exists(db_path):
    #    os.remove(db_path)
    conn = sql.get_conn(db_path)

    p_log_conn, p_monitor_conn = Pipe()
    p_log = Process(target=create_process_log,
            args=(hdfs_client, config, conn, p_log_conn))
    p_log.start()

    p_monitor = Process(target=create_process_monitor,
        args=(hdfs_client, config, conn, p_monitor_conn))
    p_monitor.start()
    #create_process_monitor(hdfs_client, config, conn, p_monitor_conn)

def create_process_log(hdfs_client, config, conn, pipe):
    LOGGER.info("start  log process")
    print "start  log process"
    from seplunk import DEBUG
    first = True
    while False if DEBUG else True:
        parse_log(hdfs_client, config, conn)
        if first:
            pipe.send(["start monitor"])
            first = False
        time.sleep(30)

def create_process_monitor(hdfs_client, config, conn, pipe):
    LOGGER.info("start monitor process")
    from seplunk import DEBUG
    first = True
    while False if DEBUG else True:
        if first:
            pipe.recv()
            print "start monitor process"
            first = False
        monitor_am(hdfs_client, config, conn)
        time.sleep(8)

#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
from xml.etree import ElementTree as ET

import pyhdfs

from core import sql
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

def save_user(conn, username, forbid=0):
    global CREATE_USER, USER_TABLE
    create_sql = ''' CREATE TABLE '%s'(
                'uid' integer(32) NOT NULL AUTO_INCREMENT,
                'username' varchar(255) NOT NULL UNIQUE,
                'forbid' tinyint(8) NOT NULL DEFAULT 0,
                CONSTRAINT 'table_primary_key' PRIMARY KEY('uid', 'username'))

    )''' % USER_TABLE
    if not CREATE_USER:
        sql.create_table(conn, create_sql)
        CREATE_USER = True
    insert_sql = "INSERT INTO '%s'('%s', '%s') values(%s, %s)" % (USER_TABLE, 'username', 'forbid', username, forbid)
    LOGGER.info(insert_sql)
    sql.execute(conn, insert_sql)



def save_job_summary(hdfs_client, conn, log_fullpath, f):
    global CREATE_JOB_SUMMARY, USER_TABLE
    jobid = f[:len(f)-8]
    summary_file = os.path.join(log_fullpath, f)
    lines = ""
    for line in hdfs_client.open(summary_file):
        lines += line
    job_info = dict((pair.split("=")) for pair in lines.split(","))
    job_info['QjobId'] = jobid
    job_info['job_checksum'] = 0
    job_info['job_runing'] = 0
    job_info['failedMaps'] = 0
    job_info['failedReduces'] = 0
    sql_table = "job_summary"
    if not CREATE_JOB_SUMMARY:
        create_sql = '''CREATE TABLE '%s' (''' % sql_table
        for key in job_info.keys():
            create_sql += "'%s' varchar(255)," % str(key)
        create_sql += '''CONSTRAINT 'job_summaryId_pri' PRIMARY KEY('user', 'QjobId'),
                        CONSTRAINT 'job_summaryId_ref' FOREIGN KEY ('user') REFERENCES %s('username')
                    )''' % USER_TABLE
        LOGGER.info("SQL:" + create_sql)
        sql.create_table(conn, create_sql)
        CREATE_JOB_SUMMARY = True
    keys, values = "", ""
    items = job_info.items()
    keys = ", ".join(map(lambda item: "'%s'" % item[0], items))
    values = ", ".join(map(lambda item: "'%s'" % item[1], items))
    #for key, value in job_info.items():
    #    keys += ("'%s',") %  key
    #    values += ("'%s',") % value

    sql_insert =  ("INSERT INTO %s(%s) VALUES (%s)") % (sql_table, keys, values)
    LOGGER.info(sql_insert)
    sql.execute(conn, sql_insert)

def save_job_conf(hdfs_client, conn, log_fullpath, f):
    global CREATE_JOB_CONF
    jobid = f[:len(f)-9]
    conf_file = os.path.join(log_fullpath, f)
    propertys = get_property(hdfs_client.open(conf_file))
    propertys["QjobId"] = jobid
    sql_table = "job_conf"
    if not CREATE_JOB_CONF:
        create_sql = "CREATE TABLE '%s' (" % sql_table
        for key in propertys.keys():
                create_sql += "'%s' varchar(255)," % key
        create_sql += "CONSTRAINT job_confId PRIMARY KEY ('QjobId'))"
        LOGGER.info(create_sql)
        sql.create_table(conn, create_sql)
        CREATE_JOB_CONF = True
    keys, values = "", ""
    items = propertys.items()
    keys = ", ".join(map(lambda item: "'%s'" % item[0], items))
    values = ", ".join(map(lambda item: "'%s'" % item[1], items))
    sql_insert =  ("INSERT INTO %s(%s) VALUES (%s)") % (sql_table, keys, values)
    LOGGER.info(sql_insert)
    sql.execute(conn, sql_insert)



def process_log(hdfs_client, config, conn):
    '''process log and save useful information in dbbase'''
    log_fullpath = config.get("mapreduce.jobhistory.intermediate-done-dir", None)
    if not log_fullpath or hdfs_client.exists(log_fullpath):
        LOGGER.error("intermediate-done-dir path not exists")
    users = hdfs_client.listdir(log_fullpath)
    for user in users:
        save_user(conn, user)
        log_fullpath = os.path.join(log_fullpath, user)
        for f in hdfs_client.listdir(log_fullpath):
            if get_extension(f) == ".summary":
                save_job_summary(hdfs_client, conn, log_fullpath, f)
            elif get_extension(f) == ".xml":
                save_job_conf(hdfs_client, conn, log_fullpath, f)
            else:
                pass


def monitor(hdfs_client, config, conn):
    '''monitor and process running mapreducess task'''
    global STAGE
    am_fullpath = config.get("yarn.app.mapreduce.am.staging-dir", None)
    if not am_fullpath or hdfs_client.exists(am_fullpath):
        LOGGER.error("intermediate-done-dir path not exists")
    users = hdfs_client.listdir(am_fullpath)
    for user in users:
        am_fullpath = os.path.join(am_fullpath, user)
        staging_path = os.path.join(am_fullpath, STAGE)
        if not hdfs_client.exists(staging):
            LOGGER.info(staging+"not exists")
        for jobid in hdfs_client.listdir(staging_path)
            LOGGER.info("user: %s jobid: %s  is running" % (user, jobid))
            running_job_path = os.path.join(staging_path, jobid)
            for f in hdfs_client.listdir(running_job_path):
                extension = get_extension(f)
                fullpath = os.path.join(running_job_path, f)
                if extension == ".jar":
                    hdfs_client.copy_to_local(fullpath, TMP_PATH)
                    jar_checksum, job_conf = process_jar(os.path.join(TMP_PATH, f)
                    save_job_summary(hdfs_client, conn)
                elif extension == ".xml":
                    if f == "job.xml":
                        pass
                    elif jobid in f:
                        pass



def process(config_path):
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
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sql.get_conn(db_path)
    process_log(hdfs_client, config, conn)
    while True:
        monitor(hdfs_client, config, conn)
        time.sleep(50)



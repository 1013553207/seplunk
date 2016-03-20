#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys
import json
from xml.etree import ElementTree as ET

import pyhdfs

from core import sql
from core.logger import get_log
from core.configure import get_configure
from core.configure import get_extension

LOGGER = get_log(__file__)

DB_PATH = "/tmp/hadoop"
DB_FLIE = "mydb.db"

CREATE_JOB_SUMMARY = False
CREATE_JOB_CONF = False

def process_log(hdfs_client, config, conn):
    log_fullpath = config.get("mapreduce.jobhistory.intermediate-done-dir", None)
    if not log_fullpath or hdfs_client.exists(log_fullpath):
        LOGGER.error("intermediate-done-dir path not exists")
    users = hdfs_client.listdir(log_fullpath)

    for user in users:
        log_fullpath = os.path.join(log_fullpath, user)
        for file in hdfs_client.listdir(log_fullpath)
            if get_extension(file) == ".summary":
                summary_file = os.path.join(log_fullpath, file)
                lines = None
                for line in hdfs_client.open(summary_file):
                    lines += line
                job_info = dict((pair.split("=")) for pair in lines.split(","))
                sql_table = "job_summary"
                if not CREATE_JOB_SUMMARY:
                    create_sql = "CREATE TABLE %s (" % sql_table
                    for key in job_info.keys():
                        create_sql += "%s varchar(255)," % key
                    create_sql += "CONSTRAINT job_summaryId PRIMARY KEY (jobId) )"
                    sql.create_table(conn, create_sql)
                    CREATE_JOB_SUMMARY = True
                keys, values = "", ""
                for key, value in job_info.items():
                    keys += (key + ",")
                    values += (value + ",")
                sql_insert =  ("INSERT INTO %s (%s) (%s)") % (sql_table, keys, values)
                sql.execute(sql_insert)
            elif get_extension(file) == ".xml":
                jobid = file[:len(file)-9]
                conf_file = os.path.join(log_fullpath, file)
                propertys = get_property(hdfs_client.open(conf_file))
                propertys["jobId"] = jobid
                sql_table = "job_conf"
                if not CREATE_JOB_CONF:
                    create_sql = "CREATE TABLE %s (" % sql_table
                    for key in propertys.keys():
                            create_sql += "%s varchar(255)," % key
                    create_sql += "CONSTRAINT job_confId PRIMARY KEY (jobId) )"
                    sql.create_table(conn, create_sql)
                    CREATE_JOB_CONF = True
                keys, values = "", ""
                for key, value in propertys.items():
                    keys += (key + ",")
                    values += (value + ",")
                sql_insert =  ("INSERT INTO %s (%s) (%s)") % (sql_table, keys, values)
                sql.execute(sql_insert)

def process(config_path):
    if not os.path.exists(config_path):
        LOGGER.error("config file path not exists")
        sys.exit(1)
    config = get_configure(config_path)
    default_fs = config.get('fs.defaultFS', None)
    if not default_fs:
        LOGGER.error("hdfs not found")
    host = default_fs.split(':').strip()
    hdfs_client = pyhdfs.HdfsClient(host)
    if not os.path.exists(DB_PATH):
        os.path.mkdir(DB_PATH)
    db_path = os.path.join(DB_PATH, DB_FLIE)
    conn = sql.get_conn(db_path)
    process_log(hdfs_client, config, conn)




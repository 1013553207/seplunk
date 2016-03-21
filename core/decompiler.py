#!/usr/bin/python

#-*- coding:utf-8 -*-

import os
import sys
import re
import uuid
import shutil
import random
import subprocess

from core.logger import get_log
from core.configure import get_extension 

JAVA = "java -jar"
DECOMPILER = "/opt/procyon-decompiler-0.5.30.jar"
MD5 = "/usr/bin/md5sum"

LOGER = get_log(__file__)


def process_jar(jar_file):
    md5 = get_md5(jar_file)
	if not os.path.exists(jar_file):
		LOGER.error("source file not exits")
		return md5, None
	tmp = os.path.join(os.getcwd(), "java_decompiler"+str(uuid.uuid1()))
	os.mkdir(tmp)
	cmdline = "%s %s -jar %s -o %s >/dev/null 2>&- 1>&-" % (JAVA, DECOMPILER, jar_file, tmp)
	LOGER.info("exec:" + cmdline)
	configure  = {}
    try:
        if os.system(cmdline):
	    	LOGER.error("error" + cmdline)
	    	return md5, None
	    for parent,dirname,files in os.walk(tmp):
	    	for f in files:
	    		fullpath = os.path.join(parent,f)
	    		LOGER.info("extension:" + fullpath)
	    		if get_extension(fullpath) == ".java":
	    			result = process_java_source(fullpath)
	    			if result:
	    				LOGER.info("add:"+str(result))
	    				configure.update(result)
                    else:
                        return md5, None
    except Exception e:
        LOGER.error(str(e))
    finally:
	    shutil.rmtree(tmp)
	return md5, configure


def process_java_source(java_file):
	LOGER.info("process:"+java_file)
	fd = open(java_file, "r")
	conf_pattern = re.compile(r"Configuration\s+(\w+)\s*=\s*new")
	conf = None
	for line in fd:
		result = conf_pattern.search(line)
		if result:
			conf = result.group(1)
			break
	set_pattern = re.compile(r"%s\.setInt\((.*)\)\s*;" % conf)
	configure = {}
	for line in fd:
		result = set_pattern.search(line)
		if result:
			key,value = result.group(1).split(",")
			configure[key] = value.strip()
	fd.close()
	return configure


def get_md5(jar_file):
	global MD5
	LOGER.info("get md5 result: "+ jar_file)
	cmdline = "%s -b  %s" % (MD5, jar_file)
	child = subprocess.Popen(cmdline, stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	return child.stdout.readline().split(" ")[0]





#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
from xml.etree import ElementTree as ET

from core.logger import get_log

LOGGER = get_log(__file__)

attribute = {'core-site.xml' : {
                "fs.defaultFS": None,
                },
             'mapred-site.xml' : {
                "yarn.app.mapreduce.am.staging-dir": None,
                "mapreduce.jobhistory.done-dir": None,
                "mapreduce.jobhistory.intermediate-done-dir": None,
                },
              'yarn-site.xml' : {
                "yarn.nodemanager.remote-app-log-dir": None,
                "yarn.log-aggregation-enable": None
              },
        }

def get_extension(path):
    return os.path.splitext(path)[1]

def get_configure(file_path):
    configure = {}
    for parent, dirname, files in os.walk(file_path):
        for f in files:
            if f in attribute.keys():
                fullpath  = os.path.join(parent, f)
                tree = ET.parse(fullpath)
                root = tree.getroot()
                file_content = attribute[f]
                keys = file_content.keys()
                for node in root:
                    name = node.find(".//name").text
                    value = node.find(".//value").text
                    if name in keys:
                        configure[name] = value
    return configure

def get_property(fd):
    tree = ET.parse(fd)
    root = tree.getroot()
    propertys = {}
    for node in root:
        name = node.find(".//name").text
        value = node.find(".//value").text
        propertys[name] = value
    return propertys

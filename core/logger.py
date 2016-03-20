#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import logging

def get_log(filename):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        # filename=os.path.join('/tmp', filename + ".log"),
                        filemode='a',
                        stream=sys.stdout)
    return logging.getLogger()

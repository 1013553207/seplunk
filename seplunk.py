#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys
import getopt

from core.core_process import seplunk_start

DEBUG = False

def set_daemon():
    # do the UNIX double-fork magic, see Stevens' "Advanced
    # Programming in the UNIX Environment" for details (ISBN 0201563177)
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            sys.exit(0)
    except OSError, e:
        print sys.stderr, "fork #1 failed: %d (%s)" % (e.errno, e.strerror)
        sys.exit(1)

    # decouple from parent environment
    # os.chdir("/")
    os.setsid()
    # os.umask(0)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent, print eventual PID before
            print "Daemon PID %d" % pid
            sys.exit(0)
    except OSError, e:
        print sys.stderr, "fork #2 failed: %d (%s)" % (e.errno, e.strerror)
        sys.exit(1)



def main():
    shortargs = 'c:h'
    useage = '''
        python    sepluck.py -h
        python     sepluck.py -c  config_path
    '''
    try:
        opts, args = getopt.getopt(sys.argv[1:], shortargs)
        if len(sys.argv) < 2:
            raise Exception("args not found")
    except :
        print useage
        sys.exit(2)


    config_path = None
    for o, a in opts:
        if o == '-c':
            config_path = os.path.realpath(a)
        elif o == '-h':
            print useage
            sys.exit(0)
        else:
            print useage
            sys.exit(1)
    if not DEBUG:
        set_daemon()
    seplunk_start(config_path)

if __name__ == '__main__':
    main()

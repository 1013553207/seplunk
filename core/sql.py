# -*- coding: utf-8 -*-
import os
import sqlite3

from core.logger import get_log

SHOW_SQL = True

LOGGER = get_log(__file__)


def get_conn(path=None):
    '''获取到数据库的连接对象，参数为数据库文件的绝对路径
    如果传递的参数是存在，并且是文件，那么就返回硬盘上面改
    路径下的数据库文件的连接对象；否则，返回内存中的数据接
    连接对象'''
    conn = sqlite3.connect(path)
    if os.path.exists(path) and os.path.isfile(path):
        LOGGER.info('硬盘上面:[{}]'.format(path))
        return conn
    else:
        conn = None
        LOGGER.info('内存上面:[:memory:]')
        return sqlite3.connect(':memory:')

def get_cursor(conn):
    '''该方法是获取数据库的游标对象，参数为数据库的连接对象
    如果数据库的连接对象不为None，则返回数据库连接对象所创
    建的游标对象；否则返回一个游标对象，该对象是内存中数据
    库连接对象所创建的游标对象'''
    if conn is not None:
        return conn.cursor()
    else:
        return get_conn('').cursor()

def drop_table(conn, table):
    '''如果表存在,则删除表，如果表中存在数据的时候，使用该
    方法的时候要慎用！'''
    if table is not None and table != '':
        sql = 'DROP TABLE IF EXISTS ' + table
        if SHOW_SQL:
            LOGGER.info('执行sql:[{}]'.format(sql))
        cu = get_cursor(conn)
        cu.execute(sql)
        conn.commit()
        LOGGER.info('删除数据库表[{}]成功!'.format(table))
        close_all(conn, cu)
    else:
        LOGGER.error('the [{}] is empty or equal None!'.format(sql))

def create_table(conn, sql):
    '''创建数据库表'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        if SHOW_SQL:
            LOGGER.info('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        conn.commit()
        LOGGER.info('创建数据库表成功!')
        close_all(conn, cu)
    else:
        LOGGER.error('the [{}] is empty or equal None!'.format(sql))



def close_all(conn, cu):
    '''关闭数据库游标对象和数据库连接对象'''
    try:
        if cu is not None:
            cu.close()
    finally:
        if cu is not None:
            cu.close()

def save(conn, sql, data):
    '''插入数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    LOGGER.info('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        LOGGER.error('the [{}] is empty or equal None!'.format(sql))

def execute(conn, sql):
    '''插入数据'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        cu.execute(sql)
        conn.commit()
        close_all(conn, cu)
    else:
        LOGGER.error('the [{}] is empty or equal None!'.format(sql))

def fetchall(conn, sql):
    '''查询所有数据'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        if SHOW_SQL:
            LOGGER.info('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        r = cu.fetchall()
        if r is not None and len(r) > 0:
            return r
        else:
            LOGGER.info('the result equal None!')
    else:
        LOGGER.error('the [{}] is empty or equal None!'.format(sql))
    return None

def fetchone(conn, sql):
    '''查询一条数据'''
    if sql is not None and sql != '':
        cu = get_cursor(conn)
        if SHOW_SQL:
            LOGGER.info('执行sql:[{}]'.format(sql))
        cu.execute(sql)
        r = cu.fetchone()
        if r is not None and  len(r) > 0:
            return r
        else:
            LOGGER.info('the result equal None!')
    else:
        LOGGER.error('the [{}] is empty or equal None!'.format(sql))
    return None



def update(conn, sql, data):
    '''更新数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    LOGGER.info('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        LOGGER.error('the [{}] is empty or equal None!'.format(sql))


def delete(conn, sql, data):
    '''删除数据'''
    if sql is not None and sql != '':
        if data is not None:
            cu = get_cursor(conn)
            for d in data:
                if SHOW_SQL:
                    LOGGER.info('执行sql:[{}],参数:[{}]'.format(sql, d))
                cu.execute(sql, d)
                conn.commit()
            close_all(conn, cu)
    else:
        LOGGER.error('the [{}] is empty or equal None!'.format(sql))

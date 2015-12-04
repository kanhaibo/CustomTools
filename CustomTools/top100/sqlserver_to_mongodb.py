# -*- coding: utf-8 -*-
'''
Created on 2015-12-1

@author: kanhaibo
'''
# import pymssql
import os
os.environ['ORACLE_HOME'] = '/Users/kanhaibo/instantclient_11_2'
os.environ['LD_LIBRARY_PATH'] = '/Users/kanhaibo/instantclient_11_2'
os.environ['PATH'] = '$PATH:/Users/kanhaibo/instantclient_11_2'
from cx_Oracle import *


if __name__ == '__main__':
    conn_oracle = cx_Oracle.connect('ele_steel', 'ele_steel', 'MAIN11')
    conn_oracle.close()
    print('ok')
#     conn = pymssql.connect(host='192.168.0.14', user='dddd', password='dddd',
#                            database='steel_db', charset='utf8')
#     conn.close()
#     print('ok')

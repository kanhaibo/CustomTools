# -*- coding: utf-8 -*-
'''
Created on 2015-4-15
@author: kanhaibo

'''
import xlrd
from pymongo import MongoClient
import os


def excel_to_mogo(dirname, db="cantonfair110", user="cantonfair",
                  passwd="cantonfair"):
    '''
    检测此文件是否存在
    '''
    if not os.path.exists(dirname):
        print('不是有效的 目录')
    else:
        l = os.listdir(dirname)
        for i in l:
            fname = ''
            if os.path.isfile(dirname + i)\
                    and i != '.DS_Store'\
                    and os.path.splitext(i)[1] != '.txt'\
                    and '~' not in os.path.splitext(i)[0]:
                fname = dirname + i
                print(i)
                xlrd.Book.encoding = 'gbk'
                bk = xlrd.open_workbook(fname)
                try:
                    sh = bk.sheet_by_index(0)
                    nrows = sh.nrows
                    ncols = sh.ncols
                    print("nrows %d, ncols %d" % (nrows, ncols))
                    conn = MongoClient('192.168.0.17:27050')
                    conn['admin'].authenticate(name='cantonfair',
                                               password='cantonfair',
                                               mechanism='MONGODB-CR')
#                     print conn['cantonfair110']['kan'].count()
#                     conn = MongoClient('mongodb://cantonfair:'
#                                         + passwrod + '@192.168.0.17:27050')
                    tempDb = conn[db]
#                     tempDb.authenticate('cantonfair', 'cantonfair',
#                                         mechanism='SCRAM-SHA-1')
#                     print conn['cantonfair110']['kan'].find().count()

#                     print tempDb
#                     tempDb.createCollection('kan')
#                     tempDb.kan.insert({"kk": "lkk"})
                    # 循环插入此文档内部的数据
                    dicHead = []
                    for pp in range(0, ncols):
                        # print(
                        #     sh.row_values(0)[pp].encode('utf-8'))
                        dicHead.append(sh.row_values(0)[pp].
                                       encode('utf-8').decode('utf-8'))
                    for ll in range(1, nrows):
                        tempDb["" + os.path.splitext(i)[0] + ""].insert(dict(
                            zip(dicHead, sh.row_values(ll))))
                    conn.close()
                except Exception as e:
                    print(e)
                finally:
                    conn.close()


if __name__ == '__main__':
    pass
    excel_to_mogo('/Users/kanhaibo/temp/top_one_hundred/', db='top_one_hundred',
                  user="cantonfair",
                  passwd="cantonfair")
    # for x in os.listdir('/Users/kanhaibo/temp/87国宏观数据'):
    #     temp_path = '/Users/kanhaibo/temp/87国宏观数据/' + x + '/'
    #     if (os.path.isdir(temp_path)):
    #         print(temp_path)
    #         excel_to_mogo(temp_path, db=x,
    #                       user="cantonfair",
    #                       passwd="cantonfair")

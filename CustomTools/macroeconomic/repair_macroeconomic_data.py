# -*- coding: utf-8 -*-
'''
Created on 2015-12-3

@author: kanhaibo
@summary: 修改宏观经济里面各个国家的日期以及货币换算
'''
from mongoengine import *
from pymongo import MongoClient
from datetime import datetime


def all_country_db():
    '''
    @summary: 拆分宏观经济里面的日期类型,为year,month,day
    '''
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['一带一路国家钢企名录']
#     for i in dbtemp.collection_names():
#         print(i)
    print(datetime.now())
    for mm in dbtemp['country_all'].find():
        print(mm['country_name'])
        for i in conn[mm['country_name']].collection_names():
            if i.split('.')[0] != 'system':
                for yy in conn[mm['country_name']][i].find():
                    try:
                        temp = yy['date']
                    except Exception as e:
                        try:
                            temp = yy['Date']
                        except Exception as e:
                            try:
                                temp = yy['指标名称']
                            except Exception as e:
                                temp = ''
                    try:
                        tempdate = datetime.strptime(temp, '%Y/%m/%d')
                    except Exception as e:
                        try:
                            tempdate = datetime.strptime(temp, '%Y-%m-%d')
                        except Exception as e:
                            try:
                                tempdate = datetime.strptime(temp, '%m月 %Y')
                            except:
                                try:
                                    tempdate = datetime.strptime(temp, '%Y/%m')
                                except:
                                    try:
                                        tempdate = datetime.strptime(temp,
                                                                     '%Y-%m')
                                    except:
                                        try:
                                            tempdate = datetime.strptime(temp,
                                                                    '%Y')
                                        except:
                                            tempdate = datetime.strptime(
                                                            '1900-1-1',
                                                             '%Y-%m-%d')
                    year = tempdate.strftime('%Y')
                    month = tempdate.strftime('%m')
                    day = tempdate.strftime('%d')
                    if temp == '':
                        pass
                    else:
                        conn[mm['country_name']][i].update(
                                            {'_id': yy['_id']},
                                            {'$set': {'year': year,
                                                      'month': month,
                                                      'day': day}}
                                            )
#                 print(i)
            conn.close()
    print(datetime.now())


if __name__ == '__main__':
#     出口数量商品细分
#     conn = MongoClient('192.168.0.17:27050')
#     dbtemp = conn['中国']
#     for m in dbtemp['出口数量商品细分'].find({}, ['date']):
#         print(datetime.strptime(m['date'], '%m月 %Y'))
#     print(datetime.strptime('1900-1-1', '%Y-%m-%d').strftime('%Y'))
    all_country_db()
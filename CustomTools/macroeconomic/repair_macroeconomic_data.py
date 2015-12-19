# -*- coding: utf-8 -*-
'''
Created on 2015-12-3

@author: kanhaibo
@summary: 修改宏观经济里面各个国家的日期以及货币换算
'''
from mongoengine import *
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta

connect(alias='exchange_rate',
        host='mongodb://192.168.0.17:27050/exchange_rate')
connect(alias='one_belet_and_one_road',
        host='mongodb://192.168.0.17:27050/一带一路国家钢企名录')


class country_all_info(DynamicDocument):
    '''
    @summary: 一带一路里面的所有国家的基本信息
    '''
    country_cn = StringField()
    country_en = StringField()
    meta = {'db_alias': 'one_belet_and_one_road'}


def do_country_all_info_en():
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['一带一路国家钢企名录']
    dbcolleciton = dbtemp['country_all']
    for i in country_all_info.objects():
        print(i.country_cn, i.country_en)
        # try:
        #     i.country_en = dbcolleciton.find_one(
        #         {'country_name': i.country_cn}, ['country_name_en'])['country_name_en']
        # except:
        #     i.country_en = ''
        # i.save()
    conn.close()


class exchange_rate(DynamicDocument):
    collection = StringField()
    meta = {'db_alias': 'exchange_rate'}


def do_exchange_rate():
    '''
    @summary: 修改导入数据中，有一列数据错误。
    '''
    for i in exchange_rate.objects(db='阿尔巴尼亚'):
        i.column = 'GDP'
        i.save()
        print(i['collection'], i['db'])


def all_country_exchange_rate():
    '''
    @summary: 修改各个宏观国家里面的汇率机制->百万美元
    '''
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['exchange_rate']
    print(datetime.now())
    for mm in dbtemp['exchange_rate'].find({'db': '阿富汗'}):
        # print(mm['db'], mm['collection'], mm[
        #       'column'], mm['rate'])
        import re
        for ii in conn[mm['db']][mm['collection']].find():
            try:
                tempfloat = float(re.sub(',', '', ii[mm['column']]))
            except Exception as e:
                tempfloat = 0
            tempfloat = tempfloat * mm['rate']
            conn[mm['db']][mm['collection']].update(
                {'_id': ii['_id']}, {'$set': {'exchange_result': tempfloat,
                                              'display_unit': mm['new_exchange_rate']}})
    for mm in dbtemp['exchange_rate'].find({'db': '阿富汗'}):
        # print(mm['db'], mm['collection'], mm[
        #       'column'], mm['rate'])
        import re
        for ii in conn['macroeconomic_norm'][mm['collection']].find({'norm_country_name_cn': '阿富汗'}):
            try:
                tempfloat = float(re.sub(',', '', ii[mm['column']]))
            except Exception as e:
                tempfloat = 0
            tempfloat = tempfloat * mm['rate']
            conn['macroeconomic_norm'][mm['collection']].update(
                {'_id': ii['_id']}, {'$set': {'exchange_result': tempfloat,
                                              'display_unit': mm['new_exchange_rate']}})

    conn.close()


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


def trans_str_datetime(temp):
    try:
        tempdate = datetime.strptime(temp, '%Y/%m/%d')
    except:
        try:
            tempdate = datetime.strptime(temp, '%Y-%m-%d')
        except:
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
    return year, month, day


def repair_data_bug():
    '''
@summary: 更新国家中中国里面的列，等于空的日期。
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['中国']
    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                print(tempyear, tempmonth, tempday)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_vietnam_data_bug():
    '''
@summary: 更新国家中越南里面的列，等于空的日期。
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['越南']
    tempcursor = dbtemp['工业产值'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempquarter = value.split(' ')[0].split('Q')[1]
                tempyear = value.split(' ')[1]
                # print(key, value)
                # tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['工业产值'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'quarter': tempquarter}})
    conn.close()


def repair_england_data_bug():
    '''
@summary: 修改宏观国家英国里面的错误数据-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['英国']
    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                print(tempyear, tempmonth, tempday)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_india_data_bug():
    '''
@summary: 修改宏观国家印度里面的错误数据-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['印度']
    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                print(tempyear, tempmonth, tempday)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_iran_data_bug():
    '''
@summary: 修改宏观国家伊朗里面的错误数据-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['伊朗']
    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempquarter = value.split(' ')[0].split('Q')[1]
                tempyear = value.split(' ')[1]
                # print(key, value)
                # tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'quarter': tempquarter}})
    conn.close()


def repair_singapore_data_bug():
    '''
@summary: 修改宏观国家新加坡里面的错误数据-对华出口
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['新加坡']
    tempcurosr1 = dbtemp['自华进口'].find()
    for y in tempcurosr1:
        tempid = y['_id']
        for key, value in y.items():
            if key == '时间/百万美元':
                # print(key, value)
                # tempquarter = value.split(' ')[0].split('Q')[1]
                # tempyear = value.split(' ')[1]
                # print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                print(tempyear, tempmonth, tempday)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['对华出口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                # print(key, value)
                # tempquarter = value.split(' ')[0].split('Q')[1]
                # tempyear = value.split(' ')[1]
                # print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                print(tempyear, tempmonth, tempday)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_greece_data_bug():
    '''
@summary: 修改宏观国家希腊里面的错误数据-对华出口-自华进口
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['希腊']
    tempcurosr1 = dbtemp['自华进口'].find()
    for y in tempcurosr1:
        tempid = y['_id']
        for key, value in y.items():
            if key == '时间/百万美元':
                # print(key, value)
                # tempquarter = value.split(' ')[0].split('Q')[1]
                # tempyear = value.split(' ')[1]
                # print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                print(tempyear, tempmonth, tempday)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['对华出口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                # print(key, value)
                # tempquarter = value.split(' ')[0].split('Q')[1]
                # tempyear = value.split(' ')[1]
                # print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                print(tempyear, tempmonth, tempday)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_spain_data_bug():
    '''
@summary: 修改宏观国家西班牙里面的错误数据-对华出口-自华进口-工业生产（产量）指数-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['西班牙']
    tempcurosr3 = dbtemp['工业生产（产量）指数商品细分'].find()
    for n in tempcurosr3:
        tempid = n['_id']
        for key, value in n.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcurosr2 = dbtemp['工业生产（产量）指数'].find()
    for m in tempcurosr2:
        tempid = m['_id']
        for key, value in m.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcurosr1 = dbtemp['自华进口'].find()
    for y in tempcurosr1:
        tempid = y['_id']
        for key, value in y.items():
            if key == '时间/百万美元':
                # print(key, value)
                # tempquarter = value.split(' ')[0].split('Q')[1]
                # tempyear = value.split(' ')[1]
                # print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['对华出口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                # print(key, value)
                # tempquarter = value.split(' ')[0].split('Q')[1]
                # tempyear = value.split(' ')[1]
                # print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_uzbekistan_data_bug():
    '''
@summary: 修改宏观国家乌兹别克斯坦里面的错误数据-gdp
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['乌兹别克斯坦']
    tempcursor = dbtemp['GDP'].find()
    now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'Date':
                tempnow = now + timedelta(days=value - 2)
                tempyear = tempnow.strftime('%Y')
                tempmonth = tempnow.strftime('%m')
                tempday = tempnow.strftime('%d')
                print(tempyear, tempmonth, tempday)
                # tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['GDP'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_turkey_data_bug():
    '''
@summary: 修改宏观国家土耳其里面的错误数据-出口(名义-价值)-出口金额商品细分
-对华出口-工业生产（产量）指数-工业生产（产量）指数商品细分
-进口(名义-价值)-进口金额商品细分-自华进口
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['土耳其']

    tempcursor = dbtemp['自华进口'].find()
    for m in tempcursor:
        tempid = m['_id']
        for key, value in m.items():
            if key == '时间/百万美元':
                # print(key, value)
                # tempquarter = value.split(' ')[0].split('Q')[1]
                # tempyear = value.split(' ')[1]
                # print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcurosr1 = dbtemp['进口金额商品细分'].find()
    for y in tempcurosr1:
        tempid = y['_id']
        for key, value in y.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcurosr1 = dbtemp['进口(名义-价值)'].find()
    for y in tempcurosr1:
        tempid = y['_id']
        for key, value in y.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcurosr1 = dbtemp['工业生产（产量）指数商品细分'].find()
    for y in tempcurosr1:
        tempid = y['_id']
        for key, value in y.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcurosr1 = dbtemp['工业生产（产量）指数'].find()
    for y in tempcurosr1:
        tempid = y['_id']
        for key, value in y.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['对华出口'].find()
    for m in tempcursor:
        tempid = m['_id']
        for key, value in m.items():
            if key == '时间/百万美元':
                # print(key, value)
                # tempquarter = value.split(' ')[0].split('Q')[1]
                # tempyear = value.split(' ')[1]
                # print(key, value)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcurosr1 = dbtemp['出口金额商品细分'].find()
    for y in tempcurosr1:
        tempid = y['_id']
        for key, value in y.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['出口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['出口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['出口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_thailand_data_bug():
    '''
@summary: 修改宏观国家泰国里面的错误数据-出口金额商品细分-对华出口-进口金额商品细分-进口数量商品细分-自华进口
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['泰国']
    tempcursor = dbtemp['出口金额商品细分'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['出口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['对华出口'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口金额商品细分'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['进口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['进口数量商品细分'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['进口数量商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['自华进口'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_slovakia_data_bug():
    '''
@summary: 修改宏观国家斯洛伐克里面的错误数据-出口(名义-价值)-出口金额商品细分-进口(名义-价值)-进口金额商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['斯洛伐克']
    tempcursor = dbtemp['出口(名义-价值)'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['出口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['出口金额商品细分'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['出口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口(名义-价值)'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['进口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口金额商品细分'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['进口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_srilanka_data_bug():
    '''
@summary: 修改宏观国家斯里兰卡里面的错误数据-出口-对华出口-工业生产（产量）指数-工业生产（产量）指数商品细分-进口-自华进口
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['斯里兰卡']
    tempcursor = dbtemp['出口'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['对华出口'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['工业生产（产量）指数'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['工业生产（产量）指数'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['自华进口'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_saudiarabia_data_bug():
    '''
@summary: 修改宏观国家沙特阿拉伯里面的错误数据-GDP
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['沙特阿拉伯']
    tempcursor = dbtemp['GDP'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                # tempnow = now + timedelta(days=value - 2)
                # tempyear = tempnow.strftime('%Y')
                # tempmonth = tempnow.strftime('%m')
                # tempday = tempnow.strftime('%d')
                # print(tempyear, tempmonth, tempday)
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['GDP'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_switzerland_data_bug():
    '''
@summary: 修改宏观国家瑞士里面的错误数据-工业生产（产量）指数-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['瑞士']
    tempcursor = dbtemp['工业生产（产量）指数'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempquarter = value.split(' ')[0].split('Q')[1]
                tempyear = value.split(' ')[1]
                # print(key, value)
                # tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['工业生产（产量）指数'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'quarter': tempquarter}})

    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    # now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempquarter = value.split(' ')[0].split('Q')[1]
                tempyear = value.split(' ')[1]
                # print(key, value)
                # tempyear, tempmonth, tempday = trans_str_datetime(value)
                # print(tempyear, tempmonth, tempday)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'quarter': tempquarter}})

    conn.close()


def repair_sweden_data_bug():
    '''
@summary: 修改宏观国家瑞典里面的错误数据-M1同比-出口(名义-价值)-出口金额商品细分-工业生产（产量）指数-进口(名义-价值)-进口金额商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['瑞典']
    tempcursor = dbtemp['M1同比'].find()
    now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'Date':
                tempnow = now + timedelta(days=value - 2)
                tempyear = tempnow.strftime('%Y')
                tempmonth = tempnow.strftime('%m')
                tempday = tempnow.strftime('%d')
                dbtemp['M1同比'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['出口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['出口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['出口金额商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['出口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['工业生产（产量）指数'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口金额商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_portugal_data_bug():
    '''
@summary: 修改宏观国家葡萄牙里面的错误数据-自华进口-对华出口-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['葡萄牙']

    tempcursor = dbtemp['自华进口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['对华出口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_norseland_data_bug():
    '''
@summary: 修改宏观国家挪威里面的错误数据-出口(名义-价值)-进口(名义-价值)
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['挪威']

    tempcursor = dbtemp['出口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['出口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_nepal_data_bug():
    '''
@summary: 修改宏观国家尼泊尔里面的错误数据-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['尼泊尔']

    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempquarter = value.split(' ')[0].split('Q')[1]
                tempyear = value.split(' ')[1]
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'quarter': tempquarter}})
    conn.close()


def repair_southafrica_data_bug():
    '''
@summary: 修改宏观国家南非里面的错误数据-对华出口-进口(名义-价值)-进口金额商品细分-自华进口
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['南非']

    tempcursor = dbtemp['对华出口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['自华进口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口金额商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_moldova_data_bug():
    '''
@summary: 修改宏观国家摩尔多瓦里面的错误数据-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['摩尔多瓦']

    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    conn.close()


def repair_bengal_data_bug():
    '''
@summary: 修改宏观国家孟加拉里面的错误数据-进口(名义-价值)-进口金额商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['孟加拉']

    tempcursor = dbtemp['进口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['进口金额商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_mongolia_data_bug():
    '''
@summary: 修改宏观国家蒙古国里面的错误数据-GDP-出口(名义-价值)-出口金额商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['蒙古国']

    tempcursor = dbtemp['GDP'].find()
    now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'Date':
                tempnow = now + timedelta(days=value - 2)
                tempyear = tempnow.strftime('%Y')
                tempmonth = tempnow.strftime('%m')
                tempday = tempnow.strftime('%d')
                dbtemp['GDP'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['出口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['出口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['出口金额商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['出口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_malaysia_data_bug():
    '''
@summary: 修改宏观国家马来西亚里面的错误数据-对华出口-自华进口
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['马来西亚']

    tempcursor = dbtemp['对华出口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['自华进口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_malaysia_data_bug():
    '''
@summary: 修改宏观国家科威特里面的错误数据-
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['科威特']

    tempcursor = dbtemp['对华出口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['对华出口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['自华进口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/百万美元':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_kyrghyzstan_data_bug():
    '''
@summary: 修改宏观国家吉尔吉斯斯坦里面的错误数据-
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['吉尔吉斯斯坦']

    tempcursor = dbtemp['GDP'].find()
    now = datetime.strptime('1900-1-1', '%Y-%m-%d')
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'Date':
                tempnow = now + timedelta(days=value - 2)
                tempyear = tempnow.strftime('%Y')
                tempmonth = tempnow.strftime('%m')
                tempday = tempnow.strftime('%d')
                dbtemp['GDP'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_poland_data_bug():
    '''
@summary: 修改宏观国家波兰里面的错误数据-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['波兰']

    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_poland_data_bug():
    '''
@summary: 修改宏观国家波兰里面的错误数据-工业生产（产量）指数商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['波兰']

    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_brazil_data_bug():
    '''
    @summary: 修改宏观国家巴西里面的错误数据-出口数量商品细分-进口数量商品细分-自华进口
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['巴西']

    tempcursor = dbtemp['自华进口'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '时间/':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['自华进口'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['出口数量商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['出口数量商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})
    tempcursor = dbtemp['进口数量商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == '':
                tempyear, tempmonth, tempday = trans_str_datetime(value)
                dbtemp['进口数量商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


def repair_australia_data_bug():
    '''
    @summary: 修改宏观国家澳大利亚里面的错误数据-出口数量商品细分-工业生产（产量）指数商品细分-进口数量商品细分
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['澳大利亚']

    tempcursor = dbtemp['出口数量商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempquarter = value.split(' ')[0].split('Q')[1]
                tempyear = value.split(' ')[1]
                dbtemp['出口数量商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'quarter': tempquarter}})

    tempcursor = dbtemp['工业生产（产量）指数商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempyear, tempmonth, tempday = trans_str_datetime(
                    str(int(value)))
                dbtemp['工业生产（产量）指数商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口数量商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempquarter = value.split(' ')[0].split('Q')[1]
                tempyear = value.split(' ')[1]
                dbtemp['进口数量商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'quarter': tempquarter}})

    conn.close()


def repair_uae_data_bug():
    '''
    @summary: 修改宏观国家阿联酋里面的错误数据-出口(名义-价值)-出口金额商品细分-出口数量分国别-进口(名义-价值)-进口数量分国别
    '''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    dbtemp = conn['阿联酋']

    tempcursor = dbtemp['出口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempyear, tempmonth, tempday = trans_str_datetime(
                    str(int(value)))
                dbtemp['出口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['出口金额商品细分'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempyear, tempmonth, tempday = trans_str_datetime(
                    str(int(value)))
                dbtemp['出口金额商品细分'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['出口数量分国别'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempyear, tempmonth, tempday = trans_str_datetime(
                    str(int(value)))
                dbtemp['出口数量分国别'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口(名义-价值)'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempyear, tempmonth, tempday = trans_str_datetime(
                    str(int(value)))
                dbtemp['进口(名义-价值)'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    tempcursor = dbtemp['进口数量分国别'].find()
    for x in tempcursor:
        tempid = x['_id']
        for key, value in x.items():
            if key == 'date':
                tempyear, tempmonth, tempday = trans_str_datetime(
                    str(int(value)))
                dbtemp['进口数量分国别'].update(
                    {'_id': tempid},
                    {'$set': {'year': tempyear,
                              'month': tempmonth,
                              'day': tempday}})

    conn.close()


if __name__ == '__main__':
    #     出口数量商品细分
    #     conn = MongoClient('192.168.0.17:27050')
    #     dbtemp = conn['中国']
    #     for m in dbtemp['出口数量商品细分'].find({}, ['date']):
    #         print(datetime.strptime(m['date'], '%m月 %Y'))
    #     print(datetime.strptime('1900-1-1', '%Y-%m-%d').strftime('%Y'))
    # all_country_db()
    # all_country_exchange_rate()
    # do_exchange_rate()
    # repair_data_bug()
    # do_country_all_info_en()
    # repair_vietnam_data_bug()
    # repair_england_data_bug()
    # repair_india_data_bug()
    # repair_iran_data_bug()
    # repair_singapore_data_bug()
    # repair_greece_data_bug()
    # repair_spain_data_bug()
    # repair_uzbekistan_data_bug()
    # repair_turkey_data_bug()
    # repair_thailand_data_bug()
    # repair_slovakia_data_bug()
    # repair_srilanka_data_bug()
    # repair_switzerland_data_bug()
    # repair_sweden_data_bug()
    # repair_portugal_data_bug()
    # repair_norseland_data_bug()
    # repair_nepal_data_bug()
    # repair_southafrica_data_bug()
    # repair_moldova_data_bug()
    # repair_bengal_data_bug()
    # repair_mongolia_data_bug()
    # repair_malaysia_data_bug()
    # repair_kyrghyzstan_data_bug()
    # repair_poland_data_bug()
    # repair_brazil_data_bug()
    # repair_australia_data_bug()
    # repair_uae_data_bug()
    all_country_exchange_rate()

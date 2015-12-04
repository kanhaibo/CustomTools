# -*- coding: utf-8 -*-
'''
Created on 2015-12-2

@author: kanhaibo
'''
from mongoengine import *
import sys
sys.path.append("..")
import address_to_latlnt

connect(alias='one_belet_and_one_road',
        host='mongodb://192.168.0.17:27050/一带一路国家钢企名录')
connect(alias='top_one_hundred',
        host='mongodb://192.168.0.17:27050/top_one_hundred')


# class steel_enterprises_directory(DynamicDocument):
#     Company = StringField()


class top_one_hundred(DynamicDocument):
    groupcompanyaddress = StringField()
    meta = {'db_alias': 'top_one_hundred'}


if __name__ == '__main__':
    # from pymongo import MongoClient
    # conn = MongoClient('192.168.0.17:27050')
    # dbtemp = conn['宏观钢铁行业']
    # print(dbtemp['0003模铸钢锭产量比例'].find({"2006": 3.9}).count())
    # # print(top_one_hundred.objects(国家 != '中国').count())
    for xx in top_one_hundred.objects():
        # if xx.groupcompany == '''ArcelorMittal *''':
        #     xx.groupcompanyaddress = '''24-26, Boulevard
        #   d'Avranches L-1160 Luxembourg'''
        #     xx.save()
        #         try:
        try:
            mm = str(xx.groupcompanyaddress.encode('utf-8'), 'utf-8')
        except:
            mm = str(xx.groupcompanyaddress.encode('ascii'), 'ascii')
        print(mm)
        # try:
        #     temp_gemtory = address_to_latlnt.address_to_LatLnts(
        #         mm.replace(' ', '+'))
        # except Exception as e:
        #     temp_gemtory = ''
        # print(mm, ' +++', temp_gemtory)
#         if temp_gemtory == '':
#             pass
#         else:
#             xx.groupgemorty = temp_gemtory
#             xx.save()
    print('over')
    #     xx.groupcompanyaddress))
    # xx.groupgemorty = address_to_latlnt.address_to_LatLnts('sdfsd')
    # xx.save()
    # if (xx.国家.encode('utf8') == '中国'.encode('utf-8')):
    #     print(xx['集团公司地址'].encode('utf8'))
    # xx['集团公司经纬度'] =
    # print(top_one_hundred.objects().count())

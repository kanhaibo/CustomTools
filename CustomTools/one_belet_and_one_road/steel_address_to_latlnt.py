#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2015-12-01
@summary: 更新数据库中还有address地址的经纬度,通过orm映射的方式。
@author: kanhaibo
'''
from mongoengine import *
import sys
sys.path.append("..")
import address_to_latlnt

connect(alias='OneBeltOneRoad', host='mongodb://192.168.0.17:27050/一带一路国家钢企名录')


class steel_enterprises_directory(DynamicDocument):

    """一带一路国家钢企名录集合数据"""
    HeadOffice = StringField()
    Company = StringField()
    meta = {'db_alias': 'OneBeltOneRoad'}


def headoffice_to_geometry():
    '''转换一带一路里面的headoffice到经纬度'''
    m = 0
    for i in steel_enterprises_directory.objects():
        try:
            print(i['HeadOffice'])
            try:
                tempLatLnt = address_to_latlnt.address_to_LatLnts(
                    i['HeadOffice'].replace(' ', '+'))
            except Exception as e:
                tempLatLnt = ''
            if tempLatLnt != '':
                i.geometry = tempLatLnt
                i.label_flag = 2
                i.save()
                m += 1
        except Exception as e:
            pass
    print(m)


def add_steel_enterprises_label_flag():
    '''增加label_flag列'''
    collection = steel_enterprises_directory._get_collection()
    print(collection.find({'label_flag': {'$exists': False}}).count())
    collection.update({'label_flag': {'$exists': False}},
                      {'$set': {'label_flag': 0}}, multi=True)
    print(collection.find({'label_flag': {'$exists': False}}).count())


if __name__ == '__main__':
    add_steel_enterprises_label_flag()

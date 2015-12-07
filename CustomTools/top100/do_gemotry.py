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


class top_one_hundred(DynamicDocument):
    groupcompanyaddress_en = StringField()
    meta = {'db_alias': 'top_one_hundred'}


def do_geometry():
    for temp in top_one_hundred.objects():
        tempdic = {}
        if temp.groupcompany_cn == '艾西达斯':
            # print(tempgroup_geometry)
            tempdic['location'] = {
                'lat': 41.045317, 'lng': 28.825875}

            temp.group_geometry = tempdic
            temp.save()

    collections = top_one_hundred._get_collection()
    print(collections.find({'group_geometry': {'$exists': False}}).count())
    # for n in collections.find({'group_geometry': {'$exists': True}}):
    #     print(n['group_geometry'])
    for m in collections.find({'group_geometry': {'$exists': False}}):
        print(m)
    # print(m['groupcompanylatitude'], m['groupcompany_cn'])

    #     if m['groupcompanylatitude'] != '':
    #         print(m['groupcompanylatitude'], m['groupcompanylongitude'])
    #         m.save()
    #         {'location': {'lng': 139.7631367, 'lat': 35.6791131}}
    # yy.group_geometry = '{'
    #     print(yy.groupcompanylatitude, yy.groupcompanylongitude)


def do_gemotry():
    for xx in top_one_hundred.objects():
        try:
            mm = str(xx.groupcompanyaddress_en.encode('utf-8'), 'utf-8')
        except:
            mm = str(xx.groupcompanyaddress_en.encode('ascii'), 'ascii')
        temp_gemtory = ''
        if mm != '':
            try:
                temp_gemtory = address_to_latlnt.address_to_LatLnts(
                    mm.replace(' ', '+'))
            except:
                temp_gemtory = ''
        print(mm, ' +++', temp_gemtory)
        if temp_gemtory == '':
            pass
        else:
            xx.group_geometry = temp_gemtory
            xx.save()
    print('over')


if __name__ == '__main__':
    do_geometry()

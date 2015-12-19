# -*- coding: utf-8 -*-
'''
Created on 2015-12-2

@author: kanhaibo
'''
import xlrd
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


def search_top_one_hundred():
    # xlrd.Book.encoding = 'gbk'
    # bk = xlrd.open_workbook(
    #     '/Users/kanhaibo/temp/top_one_hundred/top100子公司地址.xlsx')
    # sh = bk.sheet_by_index(0)
    # nrows = sh.nrows
    # ncols = sh.ncols
    # print("nrows %d, ncols %d" % (nrows, ncols))
    # for i in range(1, nrows):
    #     # print(sh.row_values(i)[1], sh.row_values(i)[2])
    #     for p in top_one_hundred.objects(__raw__={'subsidiarycompany_en': sh.row_values(i)[1]}):
    #         # p.
    #         # mm = sh.row_values(i)[2].encode('utf-8').decode('utf-8')
    #         # print(address_to_latlnt.address_to_LatLnts(
    #         #     mm.replace(' ', '+')))
    #         p.subsidiarycompanyaddress_en = sh.row_values(i)[2]
    #         p.save()
            # p.location =
            # print(p.group_geometry, p.subsidiarycompanyaddress_en)

    # for m in top_one_hundred.objects(__raw__={'location': {'$exists':
    # False}, 'subsidiarycompanyaddress_en': {'$exists': False}}):
    # for m in top_one_hundred.objects(__raw__={'subsidiarycompany_cn': '谢韦尔钢铁公司北美公司'}):
    #     print(m['location'])

    for m in top_one_hundred.objects(__raw__={'location': {'$exists': False}}):
        if m['groupcompany_cn'] != '' and m['subsidiarycompany_cn'] != '':
            # if m['subsidiarycompany_cn'] == '现代海克斯公司':
            #     print('ok')
            #     m.subsidiarycompanyaddress_en = 'West Wing of Hyundai Kia Motors Building, 12 Heolleung-ro, Seocho-gu, Seoul, Korea'
            #     m.save()
            # if m['subsidiarycompany_cn'] == '友联钢铁制造有限公司':
            #     print('ok')
            #     m.subsidiarycompanyaddress_en = 'Union Steel Bldg., 890, Daechi 4-dong, Gangnam-gu, SEOUL'
            #     m.save()
            # m.subsidiarycompanyaddress_en = m.subsidiarycompanyaddress_en.replace(
            #     '，', ',')
            # m.save()
            # print(m.subsidiarycompany_en, m['subsidiarycompany_cn'])
            print(m['subsidiarycompany_cn'], m['subsidiarycompanyaddress_en'])
            if (m['subsidiarycompany_cn'] == '现代海克斯公司') or (m['subsidiarycompany_cn'] == '友联钢铁制造有限公司'):
                try:
                    m.location = address_to_latlnt.address_to_LatLnts(
                        m['subsidiarycompanyaddress_en'].replace(' ', '+'))['location']
                    m.save()
                except:
                    pass
            # print(m['groupcompany_cn'], m['subsidiarycompany_cn'],
            #       m['subsidiarycompanyaddress_en'], address_to_latlnt.address_to_LatLnts(
            #     m['subsidiarycompanyaddress_en'].replace(' ', '+'))['location'])
    #     if m['groupcompany_cn'] != '' and m['subsidiarycompany_cn'] != '' and m['country_en'] == 'China':
    #         print(m['groupcompany_cn'], m['subsidiarycompany_cn'], m['subsidiarycompanyaddress_cn'], m[
    #               'subsidiarycompanyaddress_en'], m['subsidiarylatitude'], m['subsidiarylongitude'])
    #         # tempdic['lat'] = m['subsidiarylatitude']
        # tempdic['lng'] = m['subsidiarylongitude']
        # m.location = tempdic
        # m.save()


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


def search_no_no():
    tempcount = 0
    for m in top_one_hundred.objects(__raw__={'subsidiarycompany_cn': ''}):
        print(m['groupcompany_cn'], m['no'])
        tempcount += 1
        print(tempcount)


def do_top100_groupcompany():
    groupset = set()
    tempset = set()
    for l in top_one_hundred.objects(__raw__={'subsidiarycompany_cn': ''}):
        groupset.add(l['groupcompany_cn'])
    print(len(groupset))
    for p in top_one_hundred.objects():
        if p['groupcompany_cn'] != '' and p['subsidiarycompany_cn'] != '':
            if p['groupcompany_cn'] in groupset:
                pass
            else:
                tempset.add(p['groupcompany_cn'])
    print(tempset)
    # from pymongo import MongoClient
    # conn = MongoClient('192.168.0.17:27050')
    # dbtemp = conn['top_one_hundred']
    # for kan in tempset:
    #     tempobj = top_one_hundred()
    #     tempobj.subsidiarycompany_en = ''
    #     tempobj.country_en = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['country_en']
    #     tempobj.no = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['no']
    #     tempobj.groupcompany_cn = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['groupcompany_cn']
    #     tempobj.crudedteeloutput = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['crudedteeloutput']
    #     tempobj.subsidiarylongitude = ''
    #     tempobj.groupcompanylatitude = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['groupcompanylatitude']
    #     tempobj.groupcompanyaddress_en = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['groupcompanyaddress_en']
    #     tempobj.groupcompanyaddress_cn = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['groupcompanyaddress_cn']
    #     tempobj.groupcompanylongitude = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['groupcompanylongitude']
    #     tempobj.subsidiarycompanyaddress_en = ''
    #     tempobj.country_cn = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['country_cn']
    #     tempobj.subsidiarycompany_cn = ''
    #     tempobj.subsidiarycompanyaddress_cn = ''
    #     tempobj.groupcompany_en = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['groupcompany_en']
    #     tempobj.subsidiarylatitude = ''
    #     tempobj.group_geometry = dbtemp['top_one_hundred'].find_one(
    #         {'groupcompany_cn': kan})['group_geometry']
    #     tempobj.save()
    # conn.close()


if __name__ == '__main__':
    # do_geometry()
    # search_no_no()
    do_top100_groupcompany()

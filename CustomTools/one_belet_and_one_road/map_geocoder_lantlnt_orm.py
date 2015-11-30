#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2015-11-23
@summary: 更新数据库中还有address地址的经纬度,通过orm映射的方式。
@author: kanhaibo
'''
from mongoengine import connect
from mongoengine import DynamicDocument
from mongoengine import StringField
from mongoengine import IntField
import simplejson
import datetime
import pinyin

connect(alias='OneBeltOneRoad', host='mongodb://192.168.0.17:27050/一带一路国家钢企名录')
connect(alias='chinatsi', host='mongodb://192.168.0.17:27050/chinatsi')
connect(alias='country_pakistan', host='mongodb://192.168.0.17:27050/巴基斯坦')
connect(alias='cantonfair117',
        host='mongodb://192.168.0.17:27050/cantonfair117')


class country_all(DynamicDocument):
    country_name = StringField()
    country_spell = StringField()
    country_name_en = StringField()
    meta = {'db_alias': 'OneBeltOneRoad'}


class AIIBCountry(DynamicDocument):
    AIIB_Country = StringField()
    AIIB_Country_spell = StringField()
    AIIB_Country_en = StringField()

    def clean(self):
        self.AIIB_Country_spell = pinyin.get(self.AIIB_Country)
    meta = {'db_alias': 'OneBeltOneRoad'}


class steel_enterprises_directory_country(DynamicDocument):

    country_name = StringField(required=True)
    country_name_spell = StringField()

    def clean(self):
        self.country_name_spell = pinyin.get(self.country_name)
    meta = {'db_alias': 'OneBeltOneRoad'}


class steel_enterprises_directory(DynamicDocument):
    '''
    @summary: 一带一路国家钢铁企业集合
    @param Country: 国家
    @param Company: 企业名称
    @param HeadOffice: 总部地址
    @param Tel: 电话
    @param Fax: 传真
    @param Email: 邮箱
    @param Internet: 网址
    @param Management: 企业负责人
    @param YearEstablished: 成立时间
    @param NoOfemployees: 雇员
    @param Ownership: 所有权
    @param Subsidiaries: 分公司
    @param ShiploadingFacilities: 货船装载设施
    @param Mine: 矿山及设备
    @param Works: 工厂及设备
    @param Products: 产品
    @param SalesOffices: 营业部
    @param Certifications: 认证
    @param Brand: 品牌
    @param ChemicalAnalysis: 成分分析
    @param TradeAssociationsMemberships: 行业协会会员
    '''
    Management = StringField()
    Brand = StringField()
    YearEstablished = IntField()
    Internet = StringField()
    Subsidiaries = StringField()
    Email = StringField()
    NoOfEmployees = StringField()
    Fax = StringField()
    SalesOffices = StringField()
    Company = StringField()
    ChemicalAnalysis = StringField()
    ShiploadingFacilities = StringField()
    Ownership = StringField()
    Certifications = StringField()
    Country = StringField()
    HeadOffice = StringField()
    Products = StringField()
    Works = StringField()
    Tel = StringField()
    Mine = StringField()
    TradeAssociationsMemberships = StringField()
    geometry = StringField()
    meta = {'db_alias': 'OneBeltOneRoad'}


class enterprise(DynamicDocument):
    _id = StringField()
    ESTABLISHED = StringField()
    CONTACT_MAN = IntField()
    TELEPHON = StringField()
    COMPANY = StringField()
    AREA_CODE = StringField()
    REGISTERED_FUND = StringField()
    POST_CODE = StringField()
    COMPANY_ADDRESS = StringField()
    coordinates = StringField()
    PROVINCE = StringField()
    AREA = StringField()
    meta = {'db_alias': 'chinatsi'}


def update_excel_to_mongo():
    '''
    @summary: 更新亚投行，一带一路国家名称的中英文对称
    '''
    from pymongo import MongoClient
    import xlrd
    xlrd.Book.encoding = 'gbk'
    bk = xlrd.open_workbook('/Users/kanhaibo/Downloads/亚投行国家名称中英文.xlsx')
    bk1 = xlrd.open_workbook('/Users/kanhaibo/Downloads/一带一路国家名称中英文.xlsx')
    sh = bk.sheet_by_index(0)
    sh1 = bk1.sheet_by_index(0)
    print(sh1.nrows)
    print(sh1.ncols)
    conn = MongoClient('192.168.100.222:27017')
    db = conn['一带一路国家钢企名录']
    for xx in range(0, sh.nrows):
        print(sh.row_values(xx))
        db['AIIBCountry'].update({'AIIB_Country': sh.row_values(xx)[0]},
                             {'$set': {'AIIB_Country_en': sh.row_values(xx)[1]}
                             })
    for yy in range(0, sh1.nrows):
        print(sh1.row_values(yy))
        db['steel_enterprises_directory_country'].\
                    update({'country_name': sh1.row_values(yy)[0]},
                           {'$set': {'country_name_en': sh1.row_values(yy)[1]}
                           })
    conn.close()


def update_collection_to_collection():
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    db = conn['一带一路国家钢企名录']
    temp_aiib = db['AIIBCountry'].find({},
                    ['AIIB_Country', 'AIIB_Country_spell', 'AIIB_Country_en'])
    temp_country = db['steel_enterprises_directory_country'].find({},
                    ['country_name', 'country_name_spell', 'country_name_en'])
    tempset = set()
    for m in temp_aiib:
        temp_object = country_all()
        temp_object.country_name = m['AIIB_Country']
        temp_object.country_spell = m['AIIB_Country_spell']
        temp_object.country_name_en = m['AIIB_Country_en']
        temp_object.save()
        tempset.add(m['AIIB_Country'])
    for l in temp_country:
        if l['country_name'] in tempset:
            pass
        else:
            temp_object = country_all()
            temp_object.country_name = l['country_name']
            temp_object.country_spell = l['country_name_spell']
            temp_object.country_name_en = l['country_name_en']
            temp_object.save()
            tempset.add(l['country_name'])
    conn.close()


if __name__ == '__main__':
    update_collection_to_collection()
#     for i in db['steel_enterprises_directory'].find():
#         db['AIIBCountry'].update({'_id': i['_id']},
#                                 {'$set': {'AIIB_Country_spell':
#                                 pinyin.get(i['AIIB_Country'])}})
#     conn.close()
#     for p in steel_enterprises_directory.objects(Country='Pakistan'):
#         print p['label_flag']
#     for x in cpi.objects():
#         print x.Date
#         print x['CPI同比']
#     collection = steel_enterprises_directory._get_collection()

#     print collection.find({'Country': {'$exists': True}}).count()
#     print steel_enterprises_directory.objects(label_flag=1).
#sum('YearEstablished')
#     collection.update({'geometry': {'$exists': False}},
#                         {'$set': {'label_flag': 0}}, multi=True)
#     vtemp = steel_enterprises_directory._get_collection()
#     for m in vtemp.find({'geometry': {'$exists': False}}):
#         print m['Company']
#         print m['HeadOffice']
#         print m[u'国家']
#     print hy.find({'HY': {"$exists": True}}).count()
#     for x in  steel_enterprises_directory.objects:
#         print json.dumps(x['geometry'])

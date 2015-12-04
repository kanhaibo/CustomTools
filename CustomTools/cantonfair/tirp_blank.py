from mongoengine import *

connect(alias='cantonfair117',
        host='mongodb://192.168.0.17:27050/cantonfair117')


class 家具采购商名录(DynamicDocument):

    """docstring for 家具采购商名录"""
    来自国家 = StringField()
    meta = {'db_alias': 'cantonfair117'}


class 电子及信息采购商(DynamicDocument):

    """docstring for 家具采购商名录"""
    来自国家 = StringField()
    meta = {'db_alias': 'cantonfair117'}


class 化工产品采购商名录(DynamicDocument):

    """docstring for 家具采购商名录"""
    Country国家地区 = StringField()
    meta = {'db_alias': 'cantonfair117'}


class 玩具采购商名录(DynamicDocument):

    """docstring for 玩具采购商名录"""
    国家 = StringField()
    meta = {'db_alias': 'cantonfair117'}


def trip_blank():
    ''''''
    from pymongo import MongoClient
    conn = MongoClient('192.168.0.17:27050')
    for i in 玩具采购商名录.objects():
        try:
            i.国家 = conn['一带一路国家钢企名录']['country_all'].find_one(
                {'country_name_en': i.国家})['country_name']
            i.save()
        except Exception as e:
            print(i.国家)
            print(e)


if __name__ == '__main__':
    trip_blank()

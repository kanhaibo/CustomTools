# -*- coding: utf-8 -*-
'''
Created on 2015-11-5
comment:增加全球地图地址，直接转换成经纬度返回经纬度
@author: kanhaibo
modify_date:2015-11-11
comment:增加redis存储key，key无效以后，可以从redis里面去取下一个
@author: 阚海波
modify_date:2015-11-19
comment:增加中国地转换接口
'''

import urllib.request
import redis
from simplejson import loads
# 存放所有的key


def address_to_LatLnt(address='', key='', cn=False):
    '''
    通过传进来的地址得到经纬度
    '''
#     newaddress = 'address=%s&key=%s' % (address, key)
#     address = 'https://maps.google.cn/maps/api/geocode/json?' + newaddress
#     print(address)
#     fp = urllib.request.urlopen(
#         'https://maps.googleapis.com/maps/api/geocode/json?' + urllib.parse.quote(newaddress), timeout=2)
#
#     response = fp.read()
# # note that Python3 does not read the html code as string
# # but as html code bytearray, convert to string with
#     print(response)
#     fp.close()
    if cn:
        response = urllib.request.urlopen('https://maps.google.cn/maps/api/'
                                          'geocode/json?address=%s&key=%s'
                                          % (address, key), timeout=2).read()
    else:
        response = urllib.request.urlopen('https://maps.googleapis.com/maps/'
                                          'api/geocode/json?address=%s&key=%s'
                                          % (address, key), timeout=2).read()
    # print('stop')
    return response


def return_key():
    '''
    @summary: 返回数字最小的key，以便达到能复用的目的
    '''
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    pipe = r.pipeline()
    keys = r.keys()
    key_single = 'error'
    for m in keys:
        while 1:
            try:
                pipe.watch(m)
                current_value = pipe.get(m)
                next_value = int(current_value) + 1
                if next_value >= 2500:
                    next_value = current_value
                else:
                    key_single = m
                pipe.multi()
                pipe.set(m, next_value)
                pipe.execute()
                break
            except redis.WatchError:
                continue
        if key_single == 'error':
            pass
        else:
            break
    return key_single


def delete_key(key):
    '''
    @summary: 返回数字最小的key，以便达到能复用的目的
    '''
    r = redis.Redis(host='127.0.0.1', port=6379, db=0)
    r.delete(key)


def address_to_LatLnts(address=''):
    '''
    增加多个key循环使用的方式进行查找
    '''
    # 返回值的经纬度
    key = return_key()
    if key == 'error':
        return ''
    response = address_to_LatLnt(address, str(key, 'utf-8'))
    try:
        return loads(str(response, 'utf-8'))['results'][0]['geometry']
    except Exception as e:
        return ''
#         return json.loads(str(response.decode('utf-8')))
#.replace(' ', ' +')


if __name__ == '__main__':
    print(address_to_LatLnts(
        ''' West Wing of Hyundai Kia Motors Building, 12 Heolleung-ro, Seocho-gu, Seoul, Korea
        '''.replace(' ', '+')))

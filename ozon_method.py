import requests
import json
import datetime
import time
import log
from dateutil import parser
import pytz
timeout = 60  # таймаут 60 секунд
def ozon_import(method,apimethods, apikey,LOG_FILE,clientid):
    #делаем 5 попыток с паузой 1 минута, если не вышло пропускаем
    logers = log.get_logger(__name__, LOG_FILE)
    items = query(apimethods, logers, apikey, clientid,method)

    return items


def query(apiuri, logers, apikey,clientid,method):
    page = 1
    querycount = 500
    data,querycount = makedata(page, querycount,method)
    headers = {'Api-Key': apikey, 'Client-Id': clientid}
    res = make_query('post', apiuri, data, headers, logers)
    js = json.loads(res.text)
    # фильтруем то что уже есть
    if method=='stock' or method=='price':
        items = js['result']['items']

    else:
        items=js['result']
    itemstotal=items
    while len(items) == querycount:
        # количество записей видимо больше запросим следующую страниц
            page=page+1            #
            data,querycount = makedata(page, querycount,method)
            res = make_query('post', apiuri, data, headers, logers)
            js = json.loads(res.text)
            if method=='stock' or method=='price':
                items = js['result']['items']
            else:
                items = js['result']
            #дополним последующими записями
            itemstotal = itemstotal + items
    for el in itemstotal:
        el['ozon_id'] = 'ip_shl'
        el['dateExport'] = datetime.datetime.today().isoformat()
    return itemstotal


def makedata(page, querycount,method):

    if method == 'stock' or method=='price':
        data = f'{{"page": {page},"page_size": {querycount}}}'
    elif method == 'transaction':
        data =  f'{{"filter": {{"date": {{"from": "2020-01-01T00:00:00.999Z","to": "2020-12-31T23:59:59.999Z"}},'\
        f'"transaction_type": "all"}}'\
        f',"page": {page},"page_size": {querycount}}}'
    elif method == 'orders':
        querycount=50
        ofset=(page-1)*querycount
        data =  f'{{"dir": "asc","filter": {{"since": "2020-01-01T00:00:00.999Z","to": "2020-12-31T23:59:59.999Z"}},'\
        f'"offset": {ofset},"limit": {querycount},"with": {{"barcodes":true}}}}'

    return data,querycount


def make_query(method,uri,data,headers,logers):
    result=0;
    for i in range(1, 5):

        if method=='post':
            res=requests.post(uri,data=data,headers=headers)
        else:
            res = requests.get(uri, data=data, headers=headers)

        if res.status_code == 429:
            logers.info(f"Too many requests, wait 60 sec:{uri}")
            time.sleep(timeout)
        elif res.status_code != 200:
            logers.info(f"Ошибка запроса. Статус ответа:{res.status_code}")
            break
        else:
            result = res
            break
    return result






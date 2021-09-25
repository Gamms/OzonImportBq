import requests
import json
import datetime
import time
import log
from dateutil import parser
import pytz
timeout = 60  # таймаут 60 секунд
def ozon_import(method,apimethods, apikey,LOG_FILE,clientid,dateimport,ozonid):
    #делаем 5 попыток с паузой 1 минута, если не вышло пропускаем
    logers = log.get_logger(__name__, LOG_FILE)
    items = query(apimethods, logers, apikey, clientid,method,ozonid,dateimport)

    return items


def query(apiuri, logers, apikey,clientid,method,ozon_id,dateimport):
    page = 1
    querycount = 1000
    data,querycount = makedata(page, querycount,method,dateimport)
    headers = {'Api-Key': apikey, 'Client-Id': clientid}
    res = make_query('post', apiuri, data, headers, logers)
    js = json.loads(res.text)
    # фильтруем то что уже есть
    if method=='stock' or method=='price':
        items = js['result']['items']
    if method=='transactionv3':
        items =js['result']['operations']
    else:
        items=js['result']
    itemstotal=items
    while len(items) == querycount:
        # количество записей видимо больше запросим следующую страниц
            page=page+1            #
            data,querycount = makedata(page, querycount,method,dateimport)
            res = make_query('post', apiuri, data, headers, logers)
            js = json.loads(res.text)
            if method=='stock' or method=='price':
                items = js['result']['items']
            else:
                items = js['result']
            #дополним последующими записями
            itemstotal = itemstotal + items
    if method=='orders':
        newlist = []
        for el in itemstotal:
            newdict={}
            newdict['ozon_id'] = ozon_id
            newdict['dateExport'] = datetime.datetime.today().isoformat()
            for eldict in el:
                if type(el[eldict]) is list:
                    for ellist in el[eldict]: #цикл по строкам
                        for rekellist in ellist: #цикл по реквизитам строки
                            newdict[rekellist] = ellist[rekellist]
                else:
                    newdict[eldict]=el[eldict]
            newlist.append(newdict)
        itemstotal=newlist
    else:
        for el in itemstotal:
            el['ozon_id'] = ozon_id
            el['dateExport'] = datetime.datetime.today().isoformat()
            if method == 'price':
                if el['price']['recommended_price']=='':
                    el['price']['recommended_price']=0.0
            #if method == 'transaction':
                #if type(el['order_amount']) is float:
                #    print(el['orderAmount'])
                #    el['orderAmount']=int(el['orderAmount'])


    return itemstotal


def makedata(page, querycount,method,dateimport):
    dateimportstr=dateimport.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    datenowstr=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.999Z")
    if method == 'stock' or method=='price':
        data = f'{{"page": {page},"page_size": {querycount}}}'
    elif method == 'transaction' or method == 'transactionv3' :
        #data =  f'{{"filter": {{"date": {{"from": "2020-01-01T00:00:00.999Z","to": "2020-12-31T23:59:59.999Z"}},'\
        data = f'{{"filter": {{"date": {{"from": "{dateimportstr}","to": "{datenowstr}"}},' \
                f'"transaction_type": "all"}}'\
                f',"page": {page},"page_size": {querycount}}}'
    elif method == 'orders':
        querycount=50
        ofset=(page-1)*querycount
        #data =  f'{{"dir": "asc","filter": {{"since": "2020-08-01T00:00:00.999Z","to": "2020-12-31T23:59:59.999Z"}},'\
        data = f'{{"dir": "asc","filter": {{"since": "{dateimportstr}","to": "{datenowstr}"}},' \
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
        if res.status_code == 408:
            logers.info(f"Timeout code 408, wait 5 sec:{uri}")
            time.sleep(5)

        elif res.status_code != 200:
            logers.info(f"Ошибка запроса. Статус ответа:{res.status_code}")
            break
        else:
            result = res
            break
    return result






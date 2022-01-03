import requests
import json
import datetime
import time
import log
timeout = 60  # таймаут 60 секунд
apimethods = {'transaction': 'https://api-seller.ozon.ru/v2/finance/transaction/list',
              'transactionv3': 'https://api-seller.ozon.ru/v3/finance/transaction/list',
              'stock': 'https://api-seller.ozon.ru/v1/product/info/stocks',
              'orders': 'https://api-seller.ozon.ru/v2/posting/fbs/list',
              'fbo_orders': 'https://api-seller.ozon.ru/v2/posting/fbo/list',
              'price': 'https://api-seller.ozon.ru/v1/product/info/prices'}


def ozon_import(method,apimethods, apikey,LOG_FILE,clientid,dateimport,ozonid):
    #делаем 5 попыток с паузой 1 минута, если не вышло пропускаем
    logers = log.get_logger(__name__, LOG_FILE)
    items = query(apimethods, logers, apikey, clientid,method,ozonid,dateimport)

    return items


def checkTypeFieldFloat(newdict, elfield):
    if newdict.__contains__(elfield) and type(newdict[elfield]) is not float:
        newdict[elfield] = parse_float(newdict[elfield])


def query(apiuri, logers, apikey,clientid,method,ozon_id,dateimport):
    page = 1
    querycount = 1000
    data,querycount = makedata(page, querycount,method,dateimport)
    headers = {'Api-Key': apikey, 'Client-Id': clientid}
    res = make_query('post', apiuri, data, headers, logers)
    js = json.loads(res.text)
    # фильтруем то что уже есть
    items = datablock_from_js(js, method)
    itemstotal=items
    while len(items) == querycount:
        # количество записей видимо больше запросим следующую страниц
            page=page+1            #
            data,querycount = makedata(page, querycount,method,dateimport)
            res = make_query('post', apiuri, data, headers, logers)
            js = json.loads(res.text)
            items = datablock_from_js(js, method)
            #дополним последующими записями
            itemstotal = itemstotal + items
    if method=='orders' or method == 'fbo_orders':
        #apiurlproduct = 'https://api-seller.ozon.ru/v1/product/list'#озон не отдает артикулы сразу, нужен доп запрос
        #resproduct = make_query('post', apiurlproduct, data, headers, logers)
        #jsproduct = json.loads(resproduct.text)
        #itemsproduct=jsproduct['result']['items']
        #products = {item['product_id']: item['offer_id'] for item in itemsproduct}
        newlist = []
        for el in itemstotal:
            if el.__contains__('financial_data')\
                    and type(el['financial_data']) is dict: #проверим наличие финансового блока
                for element_product_financial in el['financial_data']['products']: #пробежимся по тч товаров из финансового блока
                    postingservice=el['financial_data']['posting_services']
                    analiticsdata=el['analytics_data']
                    newdict = el|element_product_financial|postingservice
                    if not postingservice is None:
                        newdict=newdict|postingservice
                    if not analiticsdata is None:

                        newdict = newdict |analiticsdata

                    if el.__contains__('barcodes'):
                        newdict=newdict| el['barcodes']
                    newdict['ozon_id'] = ozon_id
                    newdict['dateExport'] = datetime.datetime.today().isoformat()
                    for elfield in ['total_discount_value','old_price']:
                        checkTypeFieldFloat(newdict, elfield)

                    for product in el['products']:
                        if product['sku']==newdict['product_id']:
                            newdict['offer_id']=product['offer_id']
                            newdict['offer_name'] = product['name']
                            break

                    for key, value in list(newdict.items()):#удалим ненужные элементы
                        if type(value) is list or type(value) is dict or key=='analytics_data':
                            del newdict[key]

                    newlist.append(newdict)

        itemstotal=newlist
    else:
        for el in itemstotal:

            for elfield in ['order_amount','commission_amount']:
                checkTypeFieldFloat(el, elfield)
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


def datablock_from_js(js, method):
    if method == 'stock' or method == 'price':
        items = js['result']['items']
    if method == 'transactionv3':
        items = js['result']['operations']
    else:
        items = js['result']
    return items


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
        querycount=1000
        ofset=(page-1)*querycount
        data = f'{{"dir": "asc","filter": {{"order_created_at":{{"from": "{dateimportstr}","to": "{datenowstr}"}}}},"offset": {ofset},"limit": {querycount},"with": {{"barcodes":true,"financial_data": true,"analytics_data": true}}}}'
    elif method == 'fbo_orders':
        querycount=1000
        ofset=(page-1)*querycount
        data = f'{{"dir": "asc","filter": {{"since": "{dateimportstr}","to": "{datenowstr}"}},"offset": {ofset},"limit": {querycount},"with": {{"barcodes":true,"financial_data": true,"analytics_data": true}}}}'
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


def parse_int(s):
    try:
        res = int(eval(str(s)))
        if type(res) == int:
            return res
    except:
        return 0

def parse_float(s):
    try:
        res = float(eval(str(s)))
        if type(res) == float:
            return res
    except:
        return 0.0



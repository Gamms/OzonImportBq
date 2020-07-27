import requests
import json
import datetime
import time
import log
from dateutil import parser
import pytz
timeout = 60  # таймаут 60 секунд
def wb_import(apimethods, keyb64,LOG_FILE,dateimport,maxdatechange):
    #делаем 5 попыток с паузой 1 минута, если не вышло пропускаем
    logers = log.get_logger(__name__, LOG_FILE)
    result = 0
    for i in range(1,5):
        res = requests.get(apimethods,params=[('dateFrom',dateimport.isoformat()),('key',keyb64)])
        if res.status_code == 429:
            logers.info(f"Too many requests, wait 60 sec:{apimethods}")
            time.sleep(timeout)
        elif res.status_code != 200:
            logers.info("Ошибка запроса. Статус ответа:{}".format(res.status_code))
        else:
            js = json.loads(res.text)
            #фильтруем то что уже есть
            jsfilter = list(filter(lambda x: parser.parse(x['lastChangeDate']).replace(tzinfo=pytz.UTC) >maxdatechange, js))
            if len(js) == 0:
                logers.info("Ошибка запроса. Статус ответа:{}".format(res.status_code))
            elif len(jsfilter)==0:
                logers.info(f"Нет новых данных c {maxdatechange}. Статус ответа:{res.status_code}")
            else:
                for el in jsfilter:
                    if 'number' in el and el['number']=='':
                        del el['number']

                    el['wb_id'] = 'ip_shl'
                    el['dateExport'] = datetime.datetime.today().isoformat()
                result = jsfilter
            break
    return result

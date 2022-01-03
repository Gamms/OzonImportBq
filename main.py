import click

import datetime
import dateutils
# мои модули
import bq_method
import ozon_method
import log
import yaml
from dateutil import parser
@click.command()
@click.option(
    '--jsonkey', '-jsonkey',
    help='your json google service acount file',
)
@click.option(
    '--datasetid', '-datasetid',
    help='your dataset',
)

@click.option(
    '--bufferuploadday', '-bufferuploadday',default=5,
    help='Количество дней для загрузки'
)

@click.option(
    '--logdir','-logdir',
    help='log directory',default=""
)

@click.option(
    '--configyml','-configyml',
    help='config file yml',default="config.yml"
)


def main(jsonkey,datasetid,logdir,bufferuploadday,configyml):
    """
       Утилита коммандной строки для импорта из api OZON в Google BQ
       Для импорта доступны 6 разделов :
       1. Транзакции (--method transaction)
       2. Остатки (--method stock)
       3. Заказы (--method orders)
       4. Цены (--method sales)
       You need a valid API key from OZON API and Client-ID for the tool to work and json Google Service Account
       """
    LOG_FILE=logdir+"ozon_bq.log"
    loger=log.get_logger(__name__,LOG_FILE)
    apimethods=ozon_method.apimethods
    with open(configyml) as f:
        config = yaml.safe_load(f)

    for configMethod in config['methods']:
        tablebq=configMethod['method']['bqtable']
        method=configMethod['method']['ozonmethod']
        for lkConfig in config['lks']:
            ozonid=lkConfig['lk']['bq_id']
            apikey = lkConfig['lk']['apikey']
            clientid = lkConfig['lk']['clientid']
            loger.info(f'Начало импорта из OZON {ozonid}:')
            dateimport = datetime.date.today() - dateutils.relativedelta(days=bufferuploadday)
            if tablebq=="":
                tablebq=method


            if method == 'orders' or method == 'fbo_orders' :
                fieldname = 'created_at'
                #deleteresult=bq_method.DeleteOldReport(dateimport, datasetid, jsonkey, fieldname, tablebq,ozonid)
                #bq_method.DeleteTable(method, datasetid, jsonkey)
            elif method == 'transaction':
                fieldname = 'tranDate'
                #deleteresult=bq_method.DeleteOldReport(dateimport, datasetid, jsonkey, fieldname, tablebq,ozonid)




            #dateimport = datetime.date.today() - datetime.timedelta(days=30)  # общий буфер 30 дней
            loger.info(f'Загружаем данные с:{dateimport}')
            try:
            #   js=ozon_method.ozon_import(apimethods.get(method),apikey,LOG_FILE,dateimport,maxdatechange)
                #clientid='44346'

                items=ozon_method.ozon_import(method,apimethods.get(method), apikey,LOG_FILE,clientid,dateimport,ozonid)
                if len(items)!=0:
                    loger.info(f'Чистим  данные в {tablebq} c {dateimport}')
                    if method == 'orders' or method == 'fbo_orders':
                        fieldname = 'created_at'
                        dateclean=min(items,key=lambda x:parser.parse(x['created_at']).replace(tzinfo=None))
                        deleteresult = bq_method.DeleteOldReport(dateimport, datasetid, jsonkey, fieldname, tablebq, ozonid)
                        # bq_method.DeleteTable(method, datasetid, jsonkey)
                    elif method == 'transaction':
                        fieldname = 'transaction_date'
                        deleteresult = bq_method.DeleteOldReport(dateimport, datasetid, jsonkey, fieldname, tablebq, ozonid)
                    bq_method.export_js_to_bq(items, tablebq, jsonkey, datasetid, LOG_FILE)
                else:
                    loger.info(f'Данных нет {method} c {dateimport}')


            except Exception as e:
                loger.exception("Ошибка выполнения."+e.__str__())
if __name__ == "__main__":
    main()

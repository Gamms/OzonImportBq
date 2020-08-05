import datetime
import click
import dateutils

# мои модули
import bq_method
import log
import ozon_method


#return 'Job finished.\n'

        #for el in js:
        #    print(el)

@click.command()
@click.option(
    '--apikey', '-apikey',
    help='your ozon api key',default=""
)
@click.option(
    '--clientid', '-clientid',
    help='your ozon client_id',default=""
)

@click.option(
    '--method', '-method',
    help='your ozon catalog',default=""
)

@click.option(
    '--jsonkey', '-jsonkey',
    help='your json google service acount file',
)
@click.option(
    '--datasetid', '-datasetid',
    help='your dataset',
)

@click.option(
    '--ozonid', '-ozonid',
    help='you ozon id description',
)

@click.option(
    '--bufferuploadmonth', '-bufferuploadmonth',default=1,
    help='Количество месяцев для загрузки'
)

@click.option(
    '--logdir','-logdir',
    help='log directory',default=""
)
#@click.option('--delimiter', default=";", help='Delimiter csv.', show_default=True)
def main(apikey,jsonkey,method,datasetid,logdir,ozonid,bufferuploadmonth,clientid):
    """
       Утилита коммандной строки для импорта из api OZON в Google BQ
       Для импорта доступны 4 раздела :
       1. Транзакции (--method transaction)
       2. Остатки (--method stock)
       3. Заказы (--method orders)
       4. Цены (--method price)
       You need a valid API key from OZON API and Client-ID for the tool to work and json Google Service Account
       """
    # add filemode="w" to overwrite
    #bq_method.DeleteTable('sales',datasetid,jsonkey)    bq_method.DeleteTable('invoice', datasetid, jsonkey)    bq_method.DeleteTable('orders', datasetid, jsonkey)
    #bq_method.DeleteTable('stock', datasetid, jsonkey)
    #return
    LOG_FILE=f'{logdir}{method}_wb_bq.log'
    loger=log.get_logger(__name__,LOG_FILE)
    #logging.basicConfig(filename=logdir+"wb_bq.log", level=logging.INFO, format='%(process)d|%(levelname)s|%(asctime)s|%(message)s')
    loger.info('Начало импорта из OZON:')
    #keyb64: str = 'YjAxOTkxOTMtZThjMC00NTM3LTk1M2EtMzM1OTFlOGM3NzQ3'
    apimethods = {'transaction': 'https://api-seller.ozon.ru/v2/finance/transaction/list',
                  'stock': 'https://api-seller.ozon.ru/v1/product/info/stocks',
                  'orders': 'https://api-seller.ozon.ru/v2/posting/fbs/list',
                  'price': 'https://api-seller.ozon.ru/v1/product/info/prices'}
    #datenow = datetime.datetime.now()
    #two_day = datetime.timedelta(2)  # два дня
    #datefrom = datenow - two_day
    #for method in apimethods:
    #    js= wb_method.wb_import_table(apimethods, keyb64, method)
    #    bq_method.export_js_to_bq(js, method)
    #bq_method.CreateDataSet(datasetid,jsonkey)
    #method = 'sales'

    dateimport = datetime.date.today() - dateutils.relativedelta(months=bufferuploadmonth)
    if method == 'orders':
        fieldname = 'created_at'
        deleteresult=bq_method.DeleteOldReport(dateimport, datasetid, jsonkey, fieldname, method,ozonid)
        #bq_method.DeleteTable(method, datasetid, jsonkey)
    elif method == 'transaction':
        fieldname = 'tranDate'
        deleteresult=bq_method.DeleteOldReport(dateimport, datasetid, jsonkey, fieldname, method,ozonid)




    #dateimport = datetime.date.today() - datetime.timedelta(days=30)  # общий буфер 30 дней

    try:
    #   js=ozon_method.ozon_import(apimethods.get(method),apikey,LOG_FILE,dateimport,maxdatechange)
        #clientid='44346'
        items=ozon_method.ozon_import(method,apimethods.get(method), apikey,LOG_FILE,clientid,dateimport,ozonid)
        bq_method.export_js_to_bq(items, method,jsonkey,datasetid,LOG_FILE)
    except Exception as e:
        loger.exception("Ошибка выполнения."+e.__str__())


if __name__ == "__main__":
    main()

import click
import logging
import pkg_resources.py2_warn
import datetime
# мои модули
import bq_method
import ozon_method
import log
        #return 'Job finished.\n'

        #for el in js:
        #    print(el)

@click.command()
@click.option(
    '--apikey', '-apikey',
    help='your wb api key',default=""
)
@click.option(
    '--method', '-method',
    help='your wb catalog',default=""
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
    '--tableid', '-tableid',
    help='your table',default=""
)
@click.option(
    '--lic',is_flag=True,
    help='lic flag',default=False
)
@click.option(
    '--logdir','-logdir',
    help='log directory',default=""
)
#@click.option('--delimiter', default=";", help='Delimiter csv.', show_default=True)

def main(apikey,jsonkey,method,datasetid,tableid,lic,logdir):
    """
       Утилита коммандной строки для импорта из api OZON в Google BQ
       Для импорта доступны 6 разделов :
       1. Поставки (--method invoice)
       2. Остатки (--method stock)
       3. Заказы (--method orders)
       4. Продажи (--method sales)
       5. Отчеты о продажах (--method reportsale)
       6. Отчеты о платном хранении (--method reportstock)
       You need a valid API key from WB API for the tool to work and json Google Service Account
       """
    # add filemode="w" to overwrite
    #bq_method.DeleteTable('sales',datasetid,jsonkey)    bq_method.DeleteTable('invoice', datasetid, jsonkey)    bq_method.DeleteTable('orders', datasetid, jsonkey)
    #bq_method.DeleteTable('stock', datasetid, jsonkey)
    #return
    LOG_FILE=logdir+"ozon_bq.log"
    loger=log.get_logger(__name__,LOG_FILE)
    #logging.basicConfig(filename=logdir+"wb_bq.log", level=logging.INFO, format='%(process)d|%(levelname)s|%(asctime)s|%(message)s')
    loger.info('Начало импорта из OZON:')
    #keyb64: str = 'YjAxOTkxOTMtZThjMC00NTM3LTk1M2EtMzM1OTFlOGM3NzQ3'
    apimethods = {'invoice': 'https://suppliers-stats.wildberries.ru/api/v1/supplier/incomes',
                  'stock': 'https://suppliers-stats.wildberries.ru/api/v1/supplier/stocks',
                  'orders': 'https://suppliers-stats.wildberries.ru/api/v1/supplier/orders',
                  'sales': 'https://suppliers-stats.wildberries.ru/api/v1/supplier/sales',
                  'reportsale': 'https://suppliers-stats.wildberries.ru/api/v1/supplier/reportDetailMart',
                  'reportstock': 'https://suppliers-stats.wildberries.ru/api/v1/supplier/stochrancost'}
    #datenow = datetime.datetime.now()
    #two_day = datetime.timedelta(2)  # два дня
    #datefrom = datenow - two_day
    #for method in apimethods:
    #    js= wb_method.wb_import_table(apimethods, keyb64, method)
    #    bq_method.export_js_to_bq(js, method)
    #bq_method.CreateDataSet(datasetid,jsonkey)
    #method = 'sales'
    if method == 'reportsale' or method == 'reportstock':
        #грузим 1 последний месяц
        d = datetime.date.today()
        y = d.year
        m = d.month
        m = m - 1
        dateimport = datetime.date(y, m, 1)
        if method=='reportsale':
            fieldname='sale_dt'
        else:
            fieldname = 'DayBeg'

        bq_method.DeleteOldReport(dateimport,datasetid,jsonkey,fieldname,method)
    else:
        dateimport = datetime.date.today()-datetime.timedelta(days=30) #общий буфер 30 дней

    try:
        maxdatechange = bq_method.GetMaxRecord(method, datasetid, jsonkey)
        js=ozon_method.ozon_import(apimethods.get(method),apikey,LOG_FILE,dateimport,maxdatechange)
        if js ==0:
            loger.error("Ошибка получения данных из WB")
            return

        bq_method.export_js_to_bq(js, method,jsonkey,datasetid,LOG_FILE)

    except Exception as e:
        loger.exception("Ошибка выполнения.")
if __name__ == "__main__":
    main()

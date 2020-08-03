import unittest
import ozon_method

class MyTestCase(unittest.TestCase):
    def test_makedatastock(self):
        self.assertTupleEqual(ozon_method.makedata(1,2,'stock'),('{"page": 1,"page_size": 2}',2))
    def test_makedataorders(self):
        self.assertTupleEqual(ozon_method.makedata(1,2,'orders'),
                              ('{"dir": "asc","filter": {"since": "2020-01-01T00:00:00.999Z","to": "2020-12-31T23:59:59.999Z"},'\
        '"offset": 0,"limit": 50,"with": {"barcodes":true}}',50))
    def test_makedatatransactions(self):
        self.assertTupleEqual(ozon_method.makedata(1, 2, 'transaction'),(
        '{"filter": {"date": {"from": "2020-01-01T00:00:00.999Z","to": "2020-12-31T23:59:59.999Z"},'\
        '"transaction_type": "all"}'\
        ',"page": 1,"page_size": 2}',2))


if __name__ == '__main__':
    unittest.main()

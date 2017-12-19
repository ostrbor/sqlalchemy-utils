from datetime import date, datetime
from unittest import TestCase

from sqlalchemy import MetaData, Table
from sqlalchemy.sql import select, update, insert, delete, Selectable

from api.utils.query_compiler import compile_query, quote_dates_and_str
from api.models.metro import plu as Plu
from exceptions import QueryCompilerException
from api.settings import METRO_ENGINE_CH, METRO_ENGINE_PG


class TestQueryCompiler(TestCase):
    maxDiff = None

    def setUp(self):
        meta = MetaData(bind=METRO_ENGINE_PG)
        self.table = Table('files', meta, autoload=True, schema='transport')
        self.some_date = date(year=2100, month=1, day=1)

    def test_quote_dates(self):
        params = {'date': self.some_date}

        expected = {'date': "'2100-01-01'"}
        actual = quote_dates_and_str(params)
        self.assertDictEqual(expected, actual)

    def test_quote_datetime(self):
        some_date = datetime(year=2100, month=1, day=1)
        params = {'datetime': some_date}

        expected = {'datetime': "'2100-01-01 00:00:00'"}
        actual = quote_dates_and_str(params)
        self.assertDictEqual(expected, actual)

    def test_quote_str(self):
        params = {'date': 'some_string'}

        expected = {'date': "'some_string'"}
        actual = quote_dates_and_str(params)
        self.assertDictEqual(expected, actual)

    def test_compile_core_query_select(self):
        c = self.table.c  # column
        query = select([c.created]).where(c.created == self.some_date)

        actual = compile_query(query)
        expected = "SELECT transport.files.created \n" \
                   "FROM transport.files \n" \
                   "WHERE transport.files.created = '2100-01-01'"
        self.assertEqual(expected, actual)

    def test_compile_core_query_update(self):
        c = self.table.c  # column
        query = update(self.table).where(c.created == self.some_date).values(created=str(self.some_date))

        actual = compile_query(query)
        expected = "UPDATE transport.files " \
                   "SET created='2100-01-01' " \
                   "WHERE transport.files.created = '2100-01-01'"
        self.assertEqual(expected, actual)

    def test_compile_core_query_insert(self):
        c = self.table.c  # column
        query = insert(self.table).values(created=str(self.some_date))

        actual = compile_query(query)
        expected = "INSERT INTO transport.files (created) " \
                   "VALUES ('2100-01-01')"
        self.assertEqual(expected, actual)

    def test_compile_core_query_delete(self):
        c = self.table.c  # column
        query = delete(self.table).where(c.created == self.some_date)

        actual = compile_query(query)
        expected = "DELETE FROM transport.files " \
                   "WHERE transport.files.created = '2100-01-01'"
        self.assertEqual(expected, actual)

    def test_compile_orm_query_select(self):
        query = Plu.session.query(Plu.id, Plu.name)
        expected = 'SELECT master_data.products.plu, master_data.products.title \n' \
                   'FROM master_data.products'
        actual = compile_query(query)
        self.assertEqual(expected, actual)

    def test_raises_when_unknown_dialect(self):
        with self.assertRaises(QueryCompilerException):
            compile_query(Selectable(), 'unknown')

    def test_raises_when_incorrect_type_of_query(self):
        with self.assertRaises(QueryCompilerException):
            compile_query('unknown')

    def test_compile_to_clickhouse_dialect(self):
        meta = MetaData(bind=METRO_ENGINE_CH)
        table = Table('stock_data_tmp', meta, autoload=True, schema='co')
        query = select([table])
        actual = compile_query(query, 'clickhouse')
        expected = 'SELECT date, count, shop_id, plu, created \n' \
                   'FROM co.stock_data_tmp'
        self.assertEqual(actual, expected)

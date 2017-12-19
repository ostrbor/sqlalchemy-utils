from unittest import TestCase
from sqlalchemy.sql import select
from sqlalchemy import MetaData, Table

from api.utils.query_executor import execute_query_sync
from api.settings import METRO_ENGINE_PG
from api.models.metro import brand


class TestQueryExecutor(TestCase):
    def setUp(self):
        meta = MetaData(bind=METRO_ENGINE_PG)
        self.table = Table('files', meta, autoload=True, schema='transport')

    def test_execute_query_sync_core(self):
        c = self.table.c  # column
        query = select([c.hash]).where(c.hash == None).limit(1)
        actual = execute_query_sync(query)
        expected = [{'hash': None}]
        self.assertListEqual(actual, expected)

        query = select([c.hash]).where(c.hash == None).limit(2)
        actual = execute_query_sync(query)
        expected = [{'hash': None}, {'hash': None}]
        self.assertListEqual(actual, expected)

    def test_execute_query_sync_orm(self):
        query = brand.session.query(brand).filter(brand.id == 1)
        actual = execute_query_sync(query)
        expected = [{'id': 1, 'name': '"ЭРА"'}]
        self.assertListEqual(actual, expected)

from datetime import datetime
from typing import List
from collections import namedtuple
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def string2datetime(value):
    date = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    return date


def format_handlers_doc(spec, handlers: List):
    for handler_tuple in handlers:
        handler = handler_tuple[1]
        if 'get' in handler.__dict__:
            if getattr(handler, 'request_schema', None):
                handler.get.__doc__ = handler.get.__doc__.format(handler.request_schema)
                spec.add_path(urlspec=handler_tuple)
        elif 'post' in handler.__dict__:
            if getattr(handler, 'request_schema', None):
                handler.post.__doc__ = handler.post.__doc__.format(handler.request_schema)
                spec.add_path(urlspec=handler_tuple)
        else:
            continue


class DBChanger:
    def __init__(self, models: List):
        self.models = models
        meta = namedtuple('meta', ['table_schema', 'model_metadata', 'engine'])
        self.original = [meta(table_schema=model.__table__.schema,
                              model_metadata=model.metadata,
                              engine=model.session.bind)
                         for model in self.models]

        self.sqlite_engine = create_engine('sqlite:///:memory:')
        self.session = sessionmaker(bind=self.sqlite_engine)()

    def to_sqlite(self):
        for model in self.models:
            model.session.bind = self.sqlite_engine
            model.metadata.bind = self.sqlite_engine
            model.__table__.schema = None

        self.metadata = self.models[0].metadata  # Metadata reference sqlite database

    def to_original(self):
        for model, meta in zip(self.models, self.original):
            model.session.bind = meta.engine
            model.metadata.bind = meta.engine
            model.__table__.schema = meta.table_schema

        # IMPORTANT! Prohibit using of metadata, as now it reference to original database
        self.metadata = None

from typing import Dict, Union
from datetime import datetime, date

from sqlalchemy.orm.query import Query
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Selectable
from sqlalchemy.sql.dml import Update, Insert, Delete
from sqlalchemy.dialects import registry
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy_clickhouse import base as clickhouse

from exceptions import QueryCompilerException


def compile_query(query: Union[Query, Update, Insert, Delete, Selectable],
                  dialect: str = 'postgresql') -> str:
    dialect = get_dialect(dialect)
    if isinstance(query, Query):
        q = query.statement.compile(dialect=dialect)
    elif isinstance(query, (Selectable, Update, Insert, Delete)):
        q = query.compile(dialect=dialect)
    else:
        msg = 'Query type %s is not allowed in compiler. Check query builder.' % type(query)
        raise QueryCompilerException(msg)

    q_template = str(q)
    q_params = quote_dates_and_str(q.params)
    sql = q_template % q_params
    return sql


def quote_dates_and_str(params: Dict) -> Dict:
    for k, v in params.items():
        if isinstance(v, datetime) or isinstance(v, date) or isinstance(v, str):
            params[k] = '\'' + str(v) + '\''
    return params


def get_dialect(dialect_name: str) -> DefaultDialect:
    if dialect_name.lower() in ['postgresql', 'pg']:
        dialect_obj = postgresql.dialect()
    elif dialect_name.lower() in ['clickhouse', 'ch']:
        registry.register("clickhouse", "sqlalchemy_clickhouse.base", "dialect")
        dialect_obj = clickhouse.dialect()
    else:
        msg = 'Unknown dialect %s. Cannot find it to compile query.' % dialect_name
        raise QueryCompilerException(msg)
    return dialect_obj

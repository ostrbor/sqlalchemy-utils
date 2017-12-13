from typing import Union, List, Dict

from sqlalchemy.orm.query import Query
from sqlalchemy.sql import Selectable
from sqlalchemy.sql.dml import Update, Insert, Delete

from exceptions import QueryExecutorException
from query_container import RawQueryContainer


def execute_query_sync(query: Union[Query, Update, Insert, Delete, Selectable]) -> List[Dict]:
    if isinstance(query, Query):
        res = list(query)
        result = []
        if not res:
            return [{}]
        elif isinstance(res[0], tuple):
            fields = [x['name'] for x in query.column_descriptions]
            for row in res:
                as_dict = dict(zip(fields, row))
                result.append(as_dict)
        else:
            for row in res:
                as_dict = {k: v for k, v in row.__dict__.items() if not k.startswith('_')}
                result.append(as_dict)
    elif isinstance(query, (Selectable, Update, Insert, Delete)):
        response = query.execute()
        res = response.fetchall()
        result = []
        for row in res:
            as_dict = dict(zip(query.columns.keys(), row))
            result.append(as_dict)
    elif isinstance(query, RawQueryContainer):
        session = query.session
        q = query.raw_query
        response = session.execute(q)
        res = response.fetchall()
        result = []
        for row in res:
            as_dict = dict(zip(query.fields, row))
            result.append(as_dict)
    else:
        msg = 'Query type %s is not allowed in executor. Check query builder.' % type(query)
        raise QueryExecutorException(msg)
    return result

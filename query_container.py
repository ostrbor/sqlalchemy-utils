from typing import Tuple

from sqlalchemy.orm.session import Session


class RawQueryContainer:
    # Used to pass query into query_executor
    def __init__(self, session: Session, raw_query: str, fields: Tuple[str]):
        self.session = session  # has connection info
        self.raw_query = raw_query
        self.fields = fields  # keys that map each value in row from database

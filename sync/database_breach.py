from psycopg2.extras import DictConnection, RealDictCursor

from database import create_connection_to_source, create_connection_to_destination


class DatabaseBridge:

    def __init__(self):
        self.source_conn = create_connection_to_source()
        self.destination_conn = create_connection_to_destination()

    def new_cursor(self, conn: DictConnection):
        return conn.cursor(cursor_factory=RealDictCursor)

    def close_connections(self):
        self.source_conn.close()
        self.destination_conn.close()
"""This module is responsible for creating connections to the source and destination databases."""

from psycopg2.extras import DictConnection, RealDictCursor

from database import create_connection_to_destination, create_connection_to_source


class DatabaseBridge:
    """This class is responsible for creating connections to the source and destination databases."""

    def __init__(self):
        """Initialize the DatabaseBridge class."""
        self.source_conn = create_connection_to_source()
        self.destination_conn = create_connection_to_destination()

    def new_cursor(self, conn: DictConnection) -> RealDictCursor:
        """Create a new cursor.

        :param conn:
        :return:
        """
        return conn.cursor(cursor_factory=RealDictCursor)

    def close_connections(self) -> None:
        """Close the connections to the source and destination databases.

        :return: None
        """
        self.source_conn.close()
        self.destination_conn.close()

"""This module is responsible for creating connections to the source and destination databases."""

from psycopg2.extras import DictConnection, RealDictCursor

from database import create_connection_to_destination, create_connection_to_source


class DatabaseBridge:
    """This class is responsible for creating connections to the source and destination databases."""

    def __init__(self, require_destination: bool = False):
        """Initialize the DatabaseBridge class.
        
        :param require_destination: If True, destination connection is required. 
                                   If False, destination connection is optional.
        """
        self.source_conn = create_connection_to_source()
        if require_destination:
            self.destination_conn = create_connection_to_destination()
        else:
            # Try to create destination connection, but don't fail if it's not configured
            try:
                self.destination_conn = create_connection_to_destination()
            except Exception:
                self.destination_conn = None

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
        if self.destination_conn:
            self.destination_conn.close()

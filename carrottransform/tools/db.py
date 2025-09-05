from sqlalchemy import create_engine, select

from carrottransform.tools.logger import logger_setup
from carrottransform.tools.types import DBConnParams

logger = logger_setup()


class EngineConnection:
    """Connection to an DB Engine"""

    def __init__(self, db_conn_params: DBConnParams):
        self.db_conn_params = db_conn_params
        # If db_type is postgres, we convert it to the correct driver for postgres
        if self.db_conn_params.db_type == "postgres":
            self.db_conn_params.db_type = "postgresql+psycopg2"

        self.engine = create_engine(
            f"{self.db_conn_params.db_type}://{self.db_conn_params.username}:{self.db_conn_params.password}@{self.db_conn_params.host}:{self.db_conn_params.port}/{self.db_conn_params.db_name}"
        )
        # TODO: handle error better
        self.test_connection()

    def connect(self):
        return self.engine.connect()

    def test_connection(self):
        """Test the connection to the DB Engine"""
        try:
            connection = self.connect()
            connection.execute(select(1))
            logger.info(
                f"Connection to engine {self.db_conn_params.db_type} successful"
            )
        except Exception as e:
            logger.error(f"Error testing connection to engine: {e}")
            raise e

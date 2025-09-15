from unittest.mock import MagicMock, patch

import pytest

from carrottransform.tools.db import EngineConnection
from carrottransform.tools.types import DBConnParams


class TestEngineConnection:
    @pytest.fixture
    def db_params(self):
        return DBConnParams(
            db_type="postgres",
            username="user",
            password="pass",
            host="localhost",
            port=5432,
            db_name="test_db",
            schema="test_schema",
        )

    def test_engine_connection_initialization(self, db_params):
        """Test that EngineConnection initializes correctly with postgres db_type conversion"""

        with patch("carrottransform.tools.db.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            # Mock the connection and execute for test_connection()
            mock_connection = MagicMock()
            mock_engine.connect.return_value = mock_connection

            connection = EngineConnection(db_params)

            # Verify engine was created with correct URL and postgres was converted
            mock_create_engine.assert_called_once_with(
                "postgresql+psycopg2://user:pass@localhost:5432/test_db"
            )

            # Verify db_type was converted
            assert connection.db_conn_params.db_type == "postgresql+psycopg2"

    def test_engine_connection_initialization_non_postgres(self):
        """Test that non-postgres db_type is not modified"""
        db_params_non_postgres = DBConnParams(
            db_type="trino",
            username="user",
            password="pass",
            host="localhost",
            port=8080,
            db_name="test_db",
            schema="test_schema",
        )

        with patch("carrottransform.tools.db.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            # Mock the connection and execute for test_connection()
            mock_connection = MagicMock()
            mock_engine.connect.return_value = mock_connection

            connection = EngineConnection(db_params_non_postgres)

            # Verify engine was created with correct URL and trino was not converted
            mock_create_engine.assert_called_once_with(
                "trino://user:pass@localhost:8080/test_db"
            )

            # Verify db_type was not modified
            assert connection.db_conn_params.db_type == "trino"

    def test_engine_connection_connect(self, db_params):
        """Test the connect method returns engine connection"""

        with patch("carrottransform.tools.db.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            # Mock the connection for both initialization and explicit call
            mock_connection = MagicMock()
            mock_engine.connect.return_value = mock_connection

            connection = EngineConnection(db_params)

            # Reset the mock to clear calls from initialization
            mock_engine.connect.reset_mock()

            # Test the connect method
            conn = connection.connect()

            # Verify connect was called once after reset
            mock_engine.connect.assert_called_once()
            assert conn == mock_connection

    def test_engine_connection_test_connection_success(self, db_params):
        """Test successful connection test logs correct message"""

        with (
            patch("carrottransform.tools.db.create_engine") as mock_create_engine,
            patch("carrottransform.tools.db.logger") as mock_logger,
        ):
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            # Mock successful connection
            mock_connection = MagicMock()
            mock_engine.connect.return_value = mock_connection
            # Initialize the connection
            EngineConnection(db_params)

            # Verify success log was called
            mock_logger.info.assert_called_with(
                "Connection to engine postgresql+psycopg2 successful"
            )

    def test_engine_connection_test_connection_failure(self, db_params):
        """Test connection failure raises exception and logs error"""

        with (
            patch("carrottransform.tools.db.create_engine") as mock_create_engine,
            patch("carrottransform.tools.db.logger") as mock_logger,
        ):
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            # Mock connection failure
            mock_engine.connect.side_effect = Exception("Connection failed")

            # Expect exception during initialization due to test_connection() call
            with pytest.raises(Exception, match="Connection failed"):
                EngineConnection(db_params)

            # Verify error was logged
            mock_logger.error.assert_called_with(
                "Error testing connection to engine: Connection failed"
            )

    def test_engine_connection_test_connection_explicit_call(self, db_params):
        """Test explicit call to test_connection method"""

        with patch("carrottransform.tools.db.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            # Mock successful connection for initialization
            mock_connection = MagicMock()
            mock_engine.connect.return_value = mock_connection

            connection = EngineConnection(db_params)

            # Reset mocks to test explicit call
            mock_engine.connect.reset_mock()
            mock_connection.execute.reset_mock()

            with patch("carrottransform.tools.db.logger") as mock_logger:
                # Test explicit call to test_connection
                connection.test_connection()

                # Verify connection and execute were called
                mock_engine.connect.assert_called_once()
                mock_connection.execute.assert_called_once()

                # Verify success log
                mock_logger.info.assert_called_with(
                    "Connection to engine postgresql+psycopg2 successful"
                )

    def test_engine_connection_test_connection_explicit_failure(self, db_params):
        """Test explicit call to test_connection with failure"""

        with patch("carrottransform.tools.db.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            # Mock successful connection for initialization
            mock_connection = MagicMock()
            mock_engine.connect.return_value = mock_connection

            connection = EngineConnection(db_params)

            # Now make connect fail for the explicit test
            mock_engine.connect.side_effect = Exception("Explicit connection failed")

            with patch("carrottransform.tools.db.logger") as mock_logger:
                with pytest.raises(Exception, match="Explicit connection failed"):
                    connection.test_connection()

                # Verify error was logged
                mock_logger.error.assert_called_with(
                    "Error testing connection to engine: Explicit connection failed"
                )

import os
import time
from typing import Type
from typedb.api.connection.driver import TypeDBDriver
from typedb.api.connection.session import SessionType
from typedb.api.connection.transaction import TransactionType
from typedb.common.exception import TypeDBDriverException
from bulk_loader import AsyncBulkLoader, BulkLoader
from utils import Config, Logger


class BulkLoadTest:
    def __init__(
        self,
        driver: TypeDBDriver,
        batch_size: int,
        transaction_count: int,
        config: Config,
        logger: Logger = Logger()
    ):
        self.driver = driver
        self.batch_size = batch_size
        self.transaction_count = transaction_count
        self.dataset_dir = config.dataset_dir
        self.schema_file = config.schema_file
        self.data_files = config.data_files
        self.database = config.database
        self.use_async = config.use_async
        self.test_reattempt_wait = config.test_reattempt_wait
        self.maximum_test_attempts = config.maximum_test_attempts
        self.logger = logger

    @property
    def schema(self) -> str:
        schema_path = f"{os.getcwd()}/{self.dataset_dir}/{self.schema_file}.tql"
        return open(schema_path, "r").read()

    @property
    def bulk_loader_class(self) -> Type[BulkLoader | AsyncBulkLoader]:
        if self.use_async:
            return AsyncBulkLoader
        else:
            return BulkLoader

    def run(self) -> dict[str, int | float]:
        attempt_count = 1

        while attempt_count <= self.maximum_test_attempts:
            try:
                self.logger.info(f"Using async loader: {str(self.use_async).lower()}")
                self.logger.info(f"Using batch size: {self.batch_size}")
                self.logger.info(f"Using transaction count: {self.transaction_count}")

                if self.use_async and self.batch_size > os.cpu_count():
                    self.logger.warn(f"Transaction count exceeds CPU count.")

                result: dict[str, int | float] = dict()
                self.logger.info(f"  Creating database.")

                if self.driver.databases.contains(self.database):
                    self.driver.databases.get(self.database).delete()

                self.driver.databases.create(self.database)

                with self.driver.session(self.database, SessionType.SCHEMA) as session:
                    with session.transaction(TransactionType.WRITE) as transaction:
                        self.logger.info(f"  Defining schema.")
                        transaction.query.define(self.schema)
                        transaction.commit()

                with self.driver.session(self.database, SessionType.DATA) as session:
                    for file in self.data_files:
                        self.logger.info(f"  Loading data from file: {file}.tql")
                        query_count = 0
                        data_path = f"{os.getcwd()}/{self.dataset_dir}/{file}.tql"
                        start = time.time()
                        bulk_loader = self.bulk_loader_class(data_path, session, self.batch_size, self.transaction_count)
                        bulk_loader.load()
                        query_count += bulk_loader.queries_run
                        time_elapsed = time.time() - start
                        result[f"{file}_time"] = time_elapsed
                        result[f"{file}_count"] = query_count
                        self.logger.info(f"  Data loading complete in: {time_elapsed} s")
                        self.logger.info(f"  Total queries run: {query_count}")

                return result
            except TypeDBDriverException as exception:
                self.logger.warn(f"Test failed due to TypeDB exception: {exception}")
                attempt_count += 1
                time.sleep(self.test_reattempt_wait)
                self.logger.info(f"Re-starting test. Attempt: {attempt_count}")

        self.logger.error(f"Maximum test attempts reached: {self.maximum_test_attempts}")
        raise RuntimeError(f"Maximum test attempts reached: {self.maximum_test_attempts}")

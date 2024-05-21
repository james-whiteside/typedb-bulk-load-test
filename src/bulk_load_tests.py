import os
import time
from collections.abc import Iterator
from typing import Type
from typedb.api.connection.driver import TypeDBDriver
from typedb.api.connection.session import SessionType
from typedb.api.connection.transaction import TransactionType
from typedb.common.exception import TypeDBDriverException
from src.bulk_loaders import AsyncBulkLoader, BulkLoader
from src.utils import Config, Logger


class BulkLoadTest:
    def __init__(
        self,
        driver: TypeDBDriver,
        batch_size: int,
        transaction_count: int,
        config: Config,
        logger: Logger = None
    ):
        self.driver = driver
        self.batch_size = batch_size
        self.transaction_count = transaction_count
        self.config = config

        if logger is None:
            self.logger = Logger()
        else:
            self.logger = logger

    @property
    def schema(self) -> str:
        schema_path = f"{os.getcwd()}/{self.config.dataset_dir}/{self.config.schema_file}.tql"
        return open(schema_path, "r").read()

    @property
    def bulk_loader_class(self) -> Type[BulkLoader | AsyncBulkLoader]:
        if self.config.use_async:
            return AsyncBulkLoader
        else:
            return BulkLoader

    def run(self) -> dict:
        attempt_count = 1
        self.logger.info(f"Starting test.")

        while attempt_count <= self.config.maximum_test_attempts:
            try:
                self.logger.info(f"Using async loader: {str(self.config.use_async).lower()}")
                self.logger.info(f"Using batch size: {self.batch_size}")
                self.logger.info(f"Using transaction count: {self.transaction_count}")

                if self.config.use_async and self.transaction_count > os.cpu_count():
                    self.logger.warn(f"Transaction count exceeds CPU count.")

                result = {
                    "async_loader": self.config.use_async,
                    "batch_size": self.batch_size,
                    "transaction_count": self.transaction_count,
                }

                self.logger.info(f"  Creating database.")

                if self.driver.databases.contains(self.config.database):
                    self.driver.databases.get(self.config.database).delete()

                self.driver.databases.create(self.config.database)

                with self.driver.session(self.config.database, SessionType.SCHEMA) as session:
                    with session.transaction(TransactionType.WRITE) as transaction:
                        self.logger.info(f"  Defining schema.")
                        transaction.query.define(self.schema)
                        transaction.commit()

                with self.driver.session(self.config.database, SessionType.DATA) as session:
                    for file in self.config.data_files:
                        self.logger.info(f"  Loading data from file: {file}.tql")
                        query_count = 0
                        data_path = f"{os.getcwd()}/{self.config.dataset_dir}/{file}.tql"
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
                time.sleep(self.config.test_reattempt_wait)
                self.logger.info(f"Re-starting test. Attempt: {attempt_count}")

        self.logger.error(f"Maximum test attempts reached: {self.config.maximum_test_attempts}")
        raise RuntimeError(f"Maximum test attempts reached: {self.config.maximum_test_attempts}")


class BulkLoadTestBatch:
    def __init__(
        self,
        driver: TypeDBDriver,
        config: Config,
        logger: Logger = None
    ):
        self.driver = driver
        self.config = config

        if logger is None:
            self.logger = Logger()
        else:
            self.logger = logger

    def run(self) -> Iterator[dict]:
        for batch_size in self.config.batch_sizes:
            for transaction_count in self.config.transaction_counts:
                bulk_load = BulkLoadTest(self.driver, batch_size, transaction_count, self.config, self.logger)
                result = bulk_load.run()
                yield result

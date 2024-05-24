import os
import time
from collections.abc import Iterator
from typedb.api.connection.session import SessionType
from typedb.api.connection.transaction import TransactionType
from typedb.common.exception import TypeDBDriverException
from src.bulk_loaders import BulkLoader, CarouselBulkLoader, PoolBulkLoader
from src.utils import Logger, LoaderType, Config


def _init_loader(loader_type: LoaderType, file_paths: str | list[str], batch_size: int, transaction_count: int, config: Config) -> BulkLoader:
    match loader_type:
        case LoaderType.CAROUSEL:
            constructor = CarouselBulkLoader
        case LoaderType.POOL:
            constructor = PoolBulkLoader

    kwargs = {
        "file_paths": file_paths,
        "batch_size": batch_size,
        "transaction_count": transaction_count,
        "config": config,
    }

    return constructor(**kwargs)


class BulkLoadTest:
    def __init__(self, batch_size: int, transaction_count: int, config: Config, logger: Logger = None):
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

    def run(self) -> dict:
        attempt_count = 1
        self.logger.info(f"Starting test.")

        while attempt_count <= self.config.maximum_test_attempts:
            self.logger.info(f"Using async loader: {self.config.loader_type.value}")
            self.logger.info(f"Using batch size: {self.batch_size}")
            self.logger.info(f"Using transaction count: {self.transaction_count}")

            if self.config.loader_type is LoaderType.POOL and self.transaction_count > os.cpu_count():
                self.logger.warn(f"Transaction count exceeds CPU count.")

            result = {
                "loader_type": self.config.loader_type,
                "batch_size": self.batch_size,
                "transaction_count": self.transaction_count,
            }

            try:
                with self.config.driver_type.init(self.config.addresses, self.config.username, self.config.password) as driver:
                    self.logger.info(f"  Creating database.")

                    if driver.databases.contains(self.config.database):
                        driver.databases.get(self.config.database).delete()

                    driver.databases.create(self.config.database)

                    with driver.session(self.config.database, SessionType.SCHEMA) as session:
                        with session.transaction(TransactionType.WRITE) as transaction:
                            self.logger.info(f"  Defining schema.")
                            transaction.query.define(self.schema)
                            transaction.commit()

                for file in self.config.data_files:
                    self.logger.info(f"  Loading data from file: {file}.tql")
                    query_count = 0
                    data_path = f"{os.getcwd()}/{self.config.dataset_dir}/{file}.tql"
                    start = time.time()
                    bulk_loader = _init_loader(self.config.loader_type, data_path, self.batch_size, self.transaction_count, self.config)
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
        config: Config,
        logger: Logger = None
    ):
        self.config = config

        if logger is None:
            self.logger = Logger()
        else:
            self.logger = logger

    def run(self) -> Iterator[dict]:
        for batch_size in self.config.batch_sizes:
            for transaction_count in self.config.transaction_counts:
                bulk_load = BulkLoadTest(batch_size, transaction_count, self.config, self.logger)
                result = bulk_load.run()
                yield result

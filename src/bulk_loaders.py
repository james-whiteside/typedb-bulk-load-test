from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Iterator
from multiprocessing import Pool, Queue
from typedb.api.connection.session import SessionType
from typedb.api.connection.transaction import TransactionType, TypeDBTransaction
from typedb.common.exception import TypeDBDriverException
from src.utils import DriverType, Config


class BulkLoader(ABC):
    def __init__(self, file_paths: str | list[str], batch_size: int, transaction_count: int, config: Config):
        if type(file_paths) is str:
            self.file_paths = [file_paths]
        else:
            self.file_paths = file_paths

        self.batch_size = batch_size
        self.transaction_count = transaction_count
        self._config = config
        self.queries_run = 0

    @property
    @abstractmethod
    def loader_type(self) -> str:
        ...

    @abstractmethod
    def load(self) -> None:
        ...


class CarouselBulkLoader(BulkLoader):
    def __init__(self, file_paths: str | list[str], batch_size: int, transaction_count: int, config: Config):
        super().__init__(file_paths, batch_size, transaction_count, config)
        self._driver = self._config.driver_type.init(self._config.addresses, self._config.username, self._config.password)
        self._session = self._driver.session(self._config.database, SessionType.DATA)
        self._transactions: deque[TypeDBTransaction] = deque()
        self._uncommitted_queries = 0
        self._open_transactions()

    def __del__(self):
        while self._transactions:
            try:
                self._transactions.pop().close()
            except TypeDBDriverException:
                continue
        
        try:
            self._session.close()
        except TypeDBDriverException:
            pass
        finally:
            self._driver.close()

    @property
    def loader_type(self) -> str:
        return "carousel"

    def _queries(self) -> Iterator[str]:
        for path in self.file_paths:
            with open(path, "r") as file:
                for line in file:
                    self.queries_run += 1
                    yield line

    def _open_transactions(self) -> None:
        for _ in range(self.transaction_count):
            self._transactions.append(self._session.transaction(TransactionType.WRITE))

    def _commit(self) -> None:
        while self._transactions:
            self._transactions.popleft().commit()

    def _refresh_transactions_if_batches_full(self) -> None:
        if self.batch_size is None:
            return
        elif self._uncommitted_queries >= self.batch_size * self.transaction_count:
            self._commit()
            self._uncommitted_queries = 0
            self._open_transactions()

    def _insert(self, query: str) -> None:
        transaction = self._transactions.popleft()
        transaction.query.insert(query)
        self._transactions.append(transaction)
        self._uncommitted_queries += 1
        self._refresh_transactions_if_batches_full()

    def load(self) -> None:
        for query in self._queries():
            self._insert(query)

        self._commit()


class PoolBulkLoader(BulkLoader):
    _queue_length_factor = 4

    def __init__(self, file_paths: str | list[str], batch_size: int, transaction_count: int, config: Config):
        super().__init__(file_paths, batch_size, transaction_count, config)
        self._worker_pool = Pool(self.transaction_count)
        self._batch_queue = Queue(self._queue_length_factor * self.transaction_count)

    @property
    def loader_type(self) -> str:
        return "pool"

    def _queries(self) -> Iterator[str]:
        for path in self.file_paths:
            with open(path, "r") as file:
                for line in file:
                    self.queries_run += 1
                    yield line

    def _batches(self) -> Iterator[list[str]]:
        next_batch: list[str] = list()

        for query in self._queries():
            next_batch.append(query)

            if len(next_batch) >= self.batch_size:
                yield next_batch
                next_batch: list[str] = list()

        yield next_batch

    @staticmethod
    def _add_batches_to_queue(
        batches: Iterator[list[str]],
        queue: Queue,
        pool_size: int,
    ):
        for batch in batches:
            queue.put(batch)

        for _ in range(pool_size):
            queue.put(None)

    @staticmethod
    def _load_batches_from_queue(
        queue: Queue,
        driver_type: DriverType,
        addresses: str | list[str],
        username: str,
        password: str,
        database: str,
    ):
        with driver_type.init(addresses, username, password) as driver:
            with driver.session(database, SessionType.DATA) as session:
                while True:
                    batch: str | None = queue.get()

                    if batch is None:
                        break
                    else:
                        with session.transaction(TransactionType.WRITE) as transaction:
                            for query in batch:
                                transaction.query.insert(query)

                            transaction.commit()

    def load(self) -> None:
        add_kwargs = {
            "batches": self._batches,
            "queue": self._batch_queue,
            "pool_size": self.transaction_count,
        }

        self._worker_pool.apply_async(self._add_batches_to_queue, kwds=add_kwargs)

        load_kwargs = {
            "queue": self._batch_queue,
            "driver_type": self._config.driver_type,
            "addresses": self._config.addresses,
            "username": self._config.username,
            "password": self._config.password,
            "database": self._config.database,
        }

        for _ in range(self.transaction_count):
            self._worker_pool.apply_async(self._load_batches_from_queue, kwds=load_kwargs)

        self._worker_pool.close()
        self._worker_pool.join()

import multiprocessing.connection
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Iterator
from multiprocessing import Pool, Queue, Manager
from typedb.api.connection.session import SessionType
from typedb.api.connection.transaction import TransactionType, TypeDBTransaction
from typedb.common.exception import TypeDBDriverException
from src.utils import DriverType, Config, LoaderType
from src.mp_socket_client import socket_client
multiprocessing.connection.SocketClient = socket_client(reattempt_wait=0.01)


class BulkLoader(ABC):
    def __init__(self, file_paths: str | list[str], batch_size: int, transaction_count: int, config: Config):
        if type(file_paths) is str:
            self.file_paths = [file_paths]
        else:
            self.file_paths = file_paths

        self.batch_size = batch_size
        self.transaction_count = transaction_count
        self.config = config
        self.queries_run = 0

    @property
    @abstractmethod
    def loader_type(self) -> LoaderType:
        ...

    @abstractmethod
    def load(self) -> None:
        ...


class CarouselBulkLoader(BulkLoader):
    def __init__(self, file_paths: str | list[str], batch_size: int, transaction_count: int, config: Config):
        super().__init__(file_paths, batch_size, transaction_count, config)
        self._driver = self.config.driver_type.init(self.config.addresses, self.config.username, self.config.password)
        self._session = self._driver.session(self.config.database, SessionType.DATA)
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
    def loader_type(self) -> LoaderType:
        return LoaderType.CAROUSEL

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

    @property
    def loader_type(self) -> LoaderType:
        return LoaderType.POOL

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
    def _batch_loader(
        queue: Queue,
        driver_type: DriverType,
        addresses: str | list[str],
        username: str,
        password: str,
        database: str,
    ) -> None:
        with driver_type.init(addresses, username, password) as driver:
            with driver.session(database, SessionType.DATA) as session:
                while True:
                    batch: list[str] | None = queue.get()

                    if batch is None:
                        break
                    else:
                        with session.transaction(TransactionType.WRITE) as transaction:
                            for query in batch:
                                transaction.query.insert(query)

                            transaction.commit()

    def load(self) -> None:
        with Manager() as manager:
            pool = Pool(self.transaction_count)
            queue = manager.Queue(self._queue_length_factor * self.transaction_count)

            kwargs = {
                "queue": queue,
                "driver_type": self.config.driver_type,
                "addresses": self.config.addresses,
                "username": self.config.username,
                "password": self.config.password,
                "database": self.config.database,
            }

            for _ in range(self.transaction_count):
                pool.apply_async(self._batch_loader, kwds=kwargs)

            for batch in self._batches():
                queue.put(batch)

            for _ in range(self.transaction_count):
                queue.put(None)

            pool.close()
            pool.join()

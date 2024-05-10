from collections import deque
from collections.abc import Iterator
from multiprocessing.pool import ThreadPool
from typedb.api.connection.session import TypeDBSession
from typedb.api.connection.transaction import TransactionType, TypeDBTransaction
from typedb.common.exception import TypeDBDriverException


class BulkLoader:
    def __init__(self, file_paths: str | list[str], session: TypeDBSession, batch_size: int = None, transaction_count: int = 1):
        if type(file_paths) is str:
            self.file_paths = [file_paths]
        else:
            self.file_paths = file_paths

        self.session = session
        self.batch_size = batch_size
        self.transaction_count = transaction_count
        self.queries_run = 0
        self._transactions: deque[TypeDBTransaction] = deque()
        self._uncommitted_queries = 0
        self._open_transactions()

    def __del__(self):
        while self._transactions:
            try:
                self._transactions.pop().close()
            except TypeDBDriverException:
                continue

    def _queries(self) -> Iterator[str]:
        for path in self.file_paths:
            with open(path, "r") as file:
                for line in file:
                    self.queries_run += 1
                    yield line

    def _open_transactions(self) -> None:
        for _ in range(self.transaction_count):
            self._transactions.append(self.session.transaction(TransactionType.WRITE))

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


class AsyncBulkLoader:
    def __init__(self, file_paths: str | list[str], session: TypeDBSession, batch_size: int, pool_size: int):
        if type(file_paths) is str:
            self.file_paths = [file_paths]
        else:
            self.file_paths = file_paths

        self.batch_size = batch_size
        self.session = session
        self._worker_pool = ThreadPool(pool_size)
        self.queries_run = 0

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

    def _load_batch(self, batch: list[str]) -> None:
        with self.session.transaction(TransactionType.WRITE) as transaction:
            for query in batch:
                transaction.query.insert(query)

            transaction.commit()

    def load(self) -> None:
        for batch in self._batches():
            args = (batch,)
            self._worker_pool.apply_async(self._load_batch, args=args)

        self._worker_pool.close()
        self._worker_pool.join()

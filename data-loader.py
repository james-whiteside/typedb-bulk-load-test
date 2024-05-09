import datetime
import getpass
import os
import time
from collections import deque
from typedb.api.connection.session import TypeDBSession
from typedb.api.connection.transaction import TypeDBTransaction
from typedb.common.exception import TypeDBDriverException
from typedb.driver import TypeDB, TypeDBCredential, SessionType, TransactionType

ADDRESS = input("Address: ")
USERNAME = input("Username: ")
PASSWORD = getpass.getpass()
DATABASE = "batch-load"
DATASET_DIR = "dataset"
RESULTS_DIR = "results"
SCHEMA_TQL_FILE = "schema"
DATA_TQL_FILES = ["entities", "relations"]
BATCH_SIZES = [128, 256, 512, 1024, 2048]
TRANSACTION_COUNTS = [1, 2, 4, 8, 16, 32, 64, 128, 256]


class BulkLoader:
    def __init__(self, session: TypeDBSession, batch_size: int = None, transaction_count: int = 1):
        self.session = session
        self.batch_size = batch_size
        self.transaction_count = transaction_count
        self.transactions: deque[TypeDBTransaction] = deque()
        self.uncommitted_queries = 0
        self._open_transactions()

    def __del__(self):
        while self.transactions:
            try:
                self.transactions.pop().close()
            except TypeDBDriverException:
                continue

    def _open_transactions(self) -> None:
        for _ in range(self.transaction_count):
            self.transactions.append(self.session.transaction(TransactionType.WRITE))

    def commit(self) -> None:
        while self.transactions:
            self.transactions.popleft().commit()

    def _refresh_transactions_if_batches_full(self) -> None:
        if self.batch_size is None:
            return
        elif self.uncommitted_queries >= self.batch_size * self.transaction_count:
            self.commit()
            self.uncommitted_queries = 0
            self._open_transactions()

    def insert(self, query: str) -> None:
        transaction = self.transactions.popleft()
        transaction.query.insert(query)
        self.transactions.append(transaction)
        self.uncommitted_queries += 1
        self._refresh_transactions_if_batches_full()


def bulk_load_test(credential: TypeDBCredential, batch_size: int | None, transaction_count: int) -> dict[str, int | float]:
    schema_path = f"{os.getcwd()}/{DATASET_DIR}/{SCHEMA_TQL_FILE}.tql"
    schema = open(schema_path, "r").read()
    result = dict()

    with TypeDB.cloud_driver(ADDRESS, credential) as driver:
        print(f"Creating database.")

        if driver.databases.contains(DATABASE):
            driver.databases.get(DATABASE).delete()

        driver.databases.create(DATABASE)

        with driver.session(DATABASE, SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as transaction:
                print(f"Defining schema.")
                transaction.query.define(schema)
                transaction.commit()

        with driver.session(DATABASE, SessionType.DATA) as session:
            for file in DATA_TQL_FILES:
                print(f"Loading data from file: {file}.tql")
                print(f"  Using batch size: {batch_size}")
                print(f"  Using transaction count: {transaction_count}")
                query_count = 0
                start = time.time()
                bulk_loader = BulkLoader(session, batch_size, transaction_count)
                data_path = f"{os.getcwd()}/{DATASET_DIR}/{file}.tql"

                with open(data_path, "r") as queries:
                    for query in queries:
                        bulk_loader.insert(query)
                        query_count += 1

                bulk_loader.commit()
                time_elapsed = time.time() - start
                result[f"{file}_time"] = time_elapsed
                result[f"{file}_count"] = query_count
                print(f"  Data loading complete in: {time_elapsed} s")
                print(f"  Total queries run: {query_count}")

    return result


credential = TypeDBCredential(USERNAME, PASSWORD, tls_enabled=True)
timestamp = datetime.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
os.makedirs(f"{os.getcwd()}/{RESULTS_DIR}", exist_ok=True)
output_path = f"{os.getcwd()}/{RESULTS_DIR}/{timestamp}.csv"
header = "batch_size,transaction_count"

for file in DATA_TQL_FILES:
    header += f",{file}_count,{file}_time"

with open(output_path, "w") as output:
    output.write(f"{header}\n")

    for batch_size in BATCH_SIZES:
        for transaction_count in TRANSACTION_COUNTS:
            result = bulk_load_test(credential, batch_size, transaction_count)
            entry = f"{batch_size},{transaction_count}"

            for file in DATA_TQL_FILES:
                count_key = f"{file}_count"
                time_key = f"{file}_time"
                entry += f",{result[count_key]},{result[time_key]}"

            output.write(f"{entry}\n")

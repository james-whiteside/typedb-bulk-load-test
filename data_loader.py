import datetime
import getpass
import os
import time
from typedb.common.exception import TypeDBDriverException
from typedb.driver import TypeDB, TypeDBCredential, SessionType, TransactionType
from bulk_loader import AsyncBulkLoader, BulkLoader
from logger import Logger

ADDRESSES = input("Addresses: ").split(";")
USERNAME = input("Username: ")
PASSWORD = getpass.getpass()
DATABASE = "batch-load"
DATASET_DIR = "dataset"
RESULTS_DIR = "results"
LOG_DIR = "logs"
SCHEMA_TQL_FILE = "schema"
DATA_TQL_FILES = ["entities", "relations"]
BATCH_SIZES = [128, 256, 512, 1024, 2048]
TRANSACTION_COUNTS = [1, 2, 4, 8, 16, 32, 64, 128, 256]
TEST_REATTEMPT_WAIT = 10
MAXIMUM_TEST_ATTEMPTS = 12
USE_ASYNC = True
CREDENTIAL = TypeDBCredential(USERNAME, PASSWORD, tls_enabled=True)
TIMESTAMP = datetime.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
os.makedirs(f"{os.getcwd()}/{LOG_DIR}", exist_ok=True)
LOG_PATH = f"{os.getcwd()}/{LOG_DIR}/{TIMESTAMP}.txt"
LOGGER = Logger(LOG_PATH)
os.makedirs(f"{os.getcwd()}/{RESULTS_DIR}", exist_ok=True)
OUTPUT_PATH = f"{os.getcwd()}/{RESULTS_DIR}/{TIMESTAMP}.csv"


def bulk_load_test(
    credential: TypeDBCredential,
    batch_size: int | None,
    transaction_count: int,
    use_async: bool,
) -> dict[str, int | float]:
    schema_path = f"{os.getcwd()}/{DATASET_DIR}/{SCHEMA_TQL_FILE}.tql"
    schema = open(schema_path, "r").read()
    attempt_count = 1
    LOGGER.info(f"Starting test.")

    if use_async:
        bulk_loader_class = AsyncBulkLoader
    else:
        bulk_loader_class = BulkLoader

    while attempt_count <= MAXIMUM_TEST_ATTEMPTS:
        try:
            LOGGER.info(f"Using async loader: {str(use_async).lower()}")
            LOGGER.info(f"Using batch size: {batch_size}")
            LOGGER.info(f"Using transaction count: {transaction_count}")

            if use_async and batch_size > os.cpu_count():
                LOGGER.warn(f"Transaction count exceeds CPU count.")

            result: dict[str, int | float] = dict()

            with TypeDB.cloud_driver(ADDRESSES, credential) as driver:
                LOGGER.info(f"  Creating database.")

                if driver.databases.contains(DATABASE):
                    driver.databases.get(DATABASE).delete()

                driver.databases.create(DATABASE)

                with driver.session(DATABASE, SessionType.SCHEMA) as session:
                    with session.transaction(TransactionType.WRITE) as transaction:
                        LOGGER.info(f"  Defining schema.")
                        transaction.query.define(schema)
                        transaction.commit()

                with driver.session(DATABASE, SessionType.DATA) as session:
                    for file in DATA_TQL_FILES:
                        LOGGER.info(f"  Loading data from file: {file}.tql")
                        query_count = 0
                        data_path = f"{os.getcwd()}/{DATASET_DIR}/{file}.tql"
                        start = time.time()
                        bulk_loader = bulk_loader_class(data_path, session, batch_size, transaction_count)
                        bulk_loader.load()
                        query_count += bulk_loader.queries_run
                        time_elapsed = time.time() - start
                        result[f"{file}_time"] = time_elapsed
                        result[f"{file}_count"] = query_count
                        LOGGER.info(f"  Data loading complete in: {time_elapsed} s")
                        LOGGER.info(f"  Total queries run: {query_count}")

            return result
        except TypeDBDriverException as exception:
            LOGGER.warn(f"Test failed due to TypeDB exception: {exception}")
            attempt_count += 1
            time.sleep(TEST_REATTEMPT_WAIT)
            LOGGER.info(f"Re-starting test. Attempt: {attempt_count}")

    LOGGER.error(f"Maximum test attempts reached: {MAXIMUM_TEST_ATTEMPTS}")
    raise RuntimeError(f"Maximum test attempts reached: {MAXIMUM_TEST_ATTEMPTS}")


header = "batch_size,transaction_count"

for file in DATA_TQL_FILES:
    header += f",{file}_count,{file}_time"

with open(OUTPUT_PATH, "w") as output:
    output.write(f"{header}\n")

    for batch_size in BATCH_SIZES:
        for transaction_count in TRANSACTION_COUNTS:
            result = bulk_load_test(CREDENTIAL, batch_size, transaction_count, USE_ASYNC)
            entry = f"{batch_size},{transaction_count}"

            for file in DATA_TQL_FILES:
                count_key = f"{file}_count"
                time_key = f"{file}_time"
                entry += f",{result[count_key]},{result[time_key]}"

            output.write(f"{entry}\n")

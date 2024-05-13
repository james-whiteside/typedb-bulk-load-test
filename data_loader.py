import datetime
import getpass
import os
from typedb.driver import TypeDBCredential, TypeDB

from bulk_load_test import BulkLoadTest
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

header = "batch_size,transaction_count"

for file in DATA_TQL_FILES:
    header += f",{file}_count,{file}_time"

with open(OUTPUT_PATH, "w") as output:
    output.write(f"{header}\n")

    with TypeDB.cloud_driver(ADDRESSES, CREDENTIAL) as driver:
        for batch_size in BATCH_SIZES:
            for transaction_count in TRANSACTION_COUNTS:
                bulk_load = BulkLoadTest(driver, batch_size, transaction_count, USE_ASYNC)
                result = bulk_load.run()
                entry = f"{batch_size},{transaction_count}"

                for file in DATA_TQL_FILES:
                    count_key = f"{file}_count"
                    time_key = f"{file}_time"
                    entry += f",{result[count_key]},{result[time_key]}"

                output.write(f"{entry}\n")

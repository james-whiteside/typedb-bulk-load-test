import datetime
import getpass
import os
from typedb.driver import TypeDBCredential, TypeDB
from bulk_load_test import BulkLoadTest
from utils import Config, Logger

config = Config()
password = getpass.getpass()
credential = TypeDBCredential(config.username, password, tls_enabled=True)
timestamp = datetime.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
os.makedirs(f"{os.getcwd()}/{config.logs_dir}", exist_ok=True)
log_path = f"{os.getcwd()}/{config.logs_dir}/{timestamp}.txt"
logger = Logger(log_path)
os.makedirs(f"{os.getcwd()}/{config.results_dir}", exist_ok=True)
output_path = f"{os.getcwd()}/{config.results_dir}/{timestamp}.csv"
header = "batch_size,transaction_count"

for file in config.data_files:
    header += f",{file}_count,{file}_time"

with open(output_path, "w") as output:
    output.write(f"{header}\n")

    with TypeDB.cloud_driver(config.addresses, credential) as driver:
        for batch_size in config.batch_sizes:
            for transaction_count in config.transaction_counts:
                bulk_load = BulkLoadTest(driver, batch_size, transaction_count, config)
                result = bulk_load.run()
                entry = f"{batch_size},{transaction_count}"

                for file in config.data_files:
                    count_key = f"{file}_count"
                    time_key = f"{file}_time"
                    entry += f",{result[count_key]},{result[time_key]}"

                output.write(f"{entry}\n")

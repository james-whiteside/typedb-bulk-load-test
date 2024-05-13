import datetime
import getpass
import os
from typedb.driver import TypeDBCredential, TypeDB
from bulk_load_test import BulkLoadTestBatch
from utils import Config, Logger

config = Config()
timestamp = datetime.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
password = getpass.getpass()
credential = TypeDBCredential(config.username, password, tls_enabled=True)
log_path = f"{os.getcwd()}/{config.logs_dir}/{timestamp}.txt"
output_path = f"{os.getcwd()}/{config.results_dir}/{timestamp}.csv"
os.makedirs(f"{os.getcwd()}/{config.logs_dir}", exist_ok=True)
os.makedirs(f"{os.getcwd()}/{config.results_dir}", exist_ok=True)
logger = Logger(log_path)
header = "async_loader,batch_size,transaction_count"

for file in config.data_files:
    header += f",{file}_count,{file}_time"

with TypeDB.cloud_driver(config.addresses, credential) as driver:
    with open(output_path, "w") as output:
        output.write(f"{header}\n")
        test_batch = BulkLoadTestBatch(driver, config, logger)

        for result in test_batch.run():
            entry = f"""{str(result["async_loader"]).lower()},{result["batch_size"]},{result["transaction_count"]}"""

            for file in config.data_files:
                count_key = f"{file}_count"
                time_key = f"{file}_time"
                entry += f",{result[count_key]},{result[time_key]}"

            output.write(f"{entry}\n")

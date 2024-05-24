import datetime
import os
from src.bulk_load_tests import BulkLoadTestBatch
from src.utils import Logger, Config

if __name__ == "__main__":
    for config_path in ("config.ini",):
        config = Config(config_path)
        timestamp = datetime.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
        log_path = f"{os.getcwd()}/{config.logs_dir}/{timestamp}.txt"
        output_path = f"{os.getcwd()}/{config.results_dir}/{timestamp}.csv"
        os.makedirs(f"{os.getcwd()}/{config.logs_dir}", exist_ok=True)
        os.makedirs(f"{os.getcwd()}/{config.results_dir}", exist_ok=True)
        logger = Logger(log_path)
        header = "loader_type,batch_size,transaction_count"

        for file in config.data_files:
            header += f",{file}_count,{file}_time"

        with open(output_path, "w") as output:
            output.write(f"{header}\n")
            test_batch = BulkLoadTestBatch(config, logger)

            for result in test_batch.run():
                entry = f"""{result["loader_type"]},{result["batch_size"]},{result["transaction_count"]}"""

                for file in config.data_files:
                    count_key = f"{file}_count"
                    time_key = f"{file}_time"
                    entry += f",{result[count_key]},{result[time_key]}"

                output.write(f"{entry}\n")

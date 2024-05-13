import os
import matplotlib.pyplot as pyplot
from numpy import array
from src.utils import Config


config = Config()
results: list[dict[str, float]] = list()

for file in config.result_files:
    result_path = f"{os.getcwd()}/{config.results_dir}/{file}.csv"

    with open(result_path, "r") as lines:
        header = next(lines).strip().split(",")

        for line in lines:
            entry = line.strip().split(",")
            non_numeric_keys = ("async_loader",)
            result = {key: float(value) for key, value in zip(header, entry) if key not in non_numeric_keys}
            results.append(result)

batch_sizes: list[float] = list()
transaction_counts: list[float] = list()
times_per_query: list[float] = list()

for result in results:
    batch_size = result["batch_size"]
    transaction_count = result["transaction_count"]
    total_queries = sum(result[f"{file}_count"] for file in config.data_files)
    total_time = sum(result[f"{file}_time"] for file in config.data_files)
    time_per_query = total_time / total_queries
    batch_sizes.append(batch_size)
    transaction_counts.append(transaction_count)
    times_per_query.append(time_per_query)

figure, axes = pyplot.subplots(subplot_kw={"projection": "3d"})
axes.plot_trisurf(array(batch_sizes), array(transaction_counts), array(times_per_query))
axes.set_xlabel("batch_size")
axes.set_ylabel("transaction_count")
axes.set_zlabel("time_per_query")
pyplot.show()

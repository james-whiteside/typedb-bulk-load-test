import os
import matplotlib.pyplot as pyplot
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
            result.update({key: value for key, value in zip(header, entry) if key in non_numeric_keys})
            results.append(result)

for result in results:
    result["total_count"] = sum(result[f"{file}_count"] for file in config.data_files)
    result["total_time"] = sum(result[f"{file}_time"] for file in config.data_files)
    result["total_rate"] = result["total_count"] / result["total_time"]

for series_value in sorted({result[config.series_variable] for result in results}):
    transaction_counts = [result[config.axis_variable] for result in results if result[config.series_variable] == series_value]
    total_rates = [result["total_rate"] for result in results if result[config.series_variable] == series_value]
    series_label = f"""{"async" if result["async_loader"] == "true" else "sync"} {int(series_value)}"""
    pyplot.plot(transaction_counts, total_rates, label=series_label, marker="o")

pyplot.xlabel(config.axis_variable.replace("_", " "))
pyplot.ylabel("load rate (query / s)")
pyplot.legend(title=config.series_variable.replace("_", " "))
pyplot.show()

import datetime
import os
from configparser import ConfigParser
from enum import Enum
from random import Random


class Config:
    def __init__(self, path: str = "config.ini"):
        config_path = f"{os.getcwd()}/{path}"
        parser = ConfigParser()
        parser.read(config_path)
        self.dataset_dir = self._str(parser["project"]["dataset_dir"])
        self.results_dir = self._str(parser["project"]["results_dir"])
        self.logs_dir = self._str(parser["project"]["logs_dir"])
        self.addresses = self._str_list(parser["connection"]["addresses"])
        self.username = self._str(parser["connection"]["username"])
        self.database = self._str(parser["connection"]["database"])
        self.entity_count = self._int(parser["generation"]["entity_count"])
        self.relation_count = self._int(parser["generation"]["relation_count"])
        self.attributes_per_entity = self._int(parser["generation"]["attributes_per_entity"])
        self.random_seed = self._int(parser["generation"]["random_seed"])
        self.schema_file = self._str(parser["loading"]["schema_file"])
        self.data_files = self._str_list(parser["loading"]["data_files"])
        self.use_async = self._bool(parser["loading"]["use_async"])
        self.batch_sizes = self._int_list(parser["loading"]["batch_sizes"])
        self.transaction_counts = self._int_list(parser["loading"]["transaction_counts"])
        self.test_reattempt_wait = self._int(parser["loading"]["test_reattempt_wait"])
        self.maximum_test_attempts = self._int(parser["loading"]["maximum_test_attempts"])
        self.result_files = self._str_list(parser["plotting"]["result_files"])
        self.series_variable = self._str(parser["plotting"]["series_variable"])
        self.axis_variable = self._str(parser["plotting"]["axis_variable"])

    @staticmethod
    def _str(value: str) -> str:
        return value.strip()

    @staticmethod
    def _int(value: str) -> int:
        return int(Config._str(value))

    @staticmethod
    def _bool(value: str) -> bool:
        if Config._str(value) not in ("true", "false"):
            raise ValueError(f"Boolean config value must be 'true' or 'false', not: '{Config._str(value)}'")

        return Config._str(value) == "true"

    @staticmethod
    def _str_list(value: str) -> list[str]:
        return [Config._str(item) for item in value.strip().lstrip("[").rstrip("]").split(",")]

    @staticmethod
    def _int_list(value: str) -> list[int]:
        return [Config._int(item) for item in Config._str_list(value)]


class LogLevel(Enum):
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class Logger:
    timestamp_format = "%y-%m-%d %H:%M:%S"

    def __init__(self, path: str = None):
        self.path = path

        if self.path is not None:
            self._file = open(self.path, "w")
        else:
            self._file = None

    def __del__(self):
        if self._file is not None:
            self._file.close()

    @classmethod
    def _build_log_entry(cls, message: str, log_level: LogLevel) -> str:
        timestamp = datetime.datetime.now().strftime(cls.timestamp_format)
        prefix_width = max(len(level.value) for level in LogLevel)
        prefix = f"{log_level.value: <{prefix_width}}"
        return f"{timestamp} {prefix} {message}"

    def _log(self, message: str, log_level: LogLevel) -> None:
        entry = self._build_log_entry(message, log_level)
        print(entry)

        if self._file is not None:
            self._file.write(f"{entry}\n")

    def info(self, message: str):
        self._log(message, LogLevel.INFO)

    def warn(self, message: str):
        self._log(message, LogLevel.WARN)

    def error(self, message: str):
        self._log(message, LogLevel.ERROR)


class RandomGenerator:
    char_set = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def __init__(self, seed = None):
        self.seed = seed
        self._random = Random(self.seed)
        self.random_calls = 0

    def int(self, maximum: int, minimum: int = 1) -> int:
        self.random_calls += 1
        return self._random.randint(minimum, maximum)

    def char(self, char_set: str = None) -> str:
        self.random_calls += 1

        if char_set is None:
            return self._random.choice(self.char_set)
        else:
            return self._random.choice(char_set)

    def str(self, length: int, char_set: str = None) -> str:
        return "".join(self.char(char_set) for _ in range(length))

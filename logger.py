import datetime
from enum import Enum


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

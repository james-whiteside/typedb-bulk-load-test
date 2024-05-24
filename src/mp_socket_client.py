import multiprocessing.connection
import socket
import time
import multiprocessing.managers
from collections.abc import Callable
from functools import partial

# Workaround for multiprocessing pools becoming deadlocked when the number of workers is too high.
# https://stackoverflow.com/questions/77042910/join-on-multiprocessing-pool-deadlock-due-to-errno-61-connection-refused


def _socket_client(address, reattempt_wait):
    family = multiprocessing.connection.address_type(address)
    with socket.socket(getattr(socket, family)) as s:
        s.setblocking(True)
        while True:
            try:
                s.connect(address)
                return multiprocessing.connection.Connection(s.detach())
            except ConnectionRefusedError:
                time.sleep(reattempt_wait)


def socket_client(reattempt_wait: float) -> Callable:
    return partial(_socket_client, reattempt_wait=reattempt_wait)

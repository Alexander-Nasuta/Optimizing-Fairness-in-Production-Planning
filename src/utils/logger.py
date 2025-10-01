import atexit
import json
import os
import logging.config
import pathlib as pl
import shutil

from utils.wzl_banner import wzl_banner
from utils.fairwork_banner import banner_color as fairwork_banner

# print banner when logger is imported
w, h = shutil.get_terminal_size((80, 20))
# print(f"terminal dimensions: {w}x{h}")


# print(small_banner if w < 140 else big_banner)
print(wzl_banner)
print(fairwork_banner)

log = logging.getLogger("AI-Service")
log.info(f"working directory: {os.getcwd()}")
log.setLevel(logging.DEBUG)  # Set the logger to DEBUG level

if __name__ == '__main__':

    log.info("test")
    # extra information can be added to the log message by passing a dictionary to the extra keyword argument
    # see logs/app.log.jsonl and have a look how it's different from the log above (look for extra_infor_key)
    log.info("test")
    log.debug("test")
    log.error("test")
    log.warning("test")
    try:
        1 / 0
    except ZeroDivisionError as e:
        log.exception(e)

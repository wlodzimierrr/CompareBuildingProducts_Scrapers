import os
import sys
import logging
from datetime import datetime

main_dir = os.path.dirname(os.path.abspath(__file__))

def get_logger(module_name):
    log_dir = os.path.join(main_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    if module_name == '__main__':
        log_file = os.path.join(log_dir, f'main.log')
    else:
        log_file = os.path.join(log_dir, f'{module_name.lower()}.log')

    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s', datefmt='%d-%m-%y %H:%M:%S')

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def setup_main_logger():
    main_logger = logging.getLogger('main')
    main_logger.setLevel(logging.INFO)
    main_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s', datefmt='%d-%m-%y %H:%M:%S')

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(main_formatter)
    main_logger.addHandler(console_handler)

    log_dir = os.path.join(main_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    main_log_file = os.path.join(log_dir, f'main.log')
    file_handler = logging.FileHandler(main_log_file, mode='a')
    file_handler.setFormatter(main_formatter)
    main_logger.addHandler(file_handler)

    return main_logger
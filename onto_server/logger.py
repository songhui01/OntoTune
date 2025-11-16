# logger.py
import os
from datetime import datetime

import numpy as np
import torch

_log_file = None
_log_file_name = None

def init_log(name_prefix="server_log"):
    """
    Initialize the log file. Only called once.
    The filename format is: {prefix}_{timestamp}.txt
    """
    global _log_file, _log_file_name
    if _log_file is None:
        os.makedirs("log", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        _log_file_name = f"{name_prefix}_{timestamp}.txt"
        log_path = os.path.join("log", _log_file_name)
        _log_file = open(log_path, "w")
    return _log_file

def log_matrix(name, matrix, name_prefix=None):
    """
    Log a variable (e.g., matrix or tensor) into the log file.
    If this is the first call and a custom prefix is provided, it will be used.
    Supports numpy arrays and torch tensors, and forces full content printing.
    """
    f = init_log(name_prefix if name_prefix else "server_log")

    # Convert torch tensor to numpy if needed
    if isinstance(matrix, torch.Tensor):
        matrix = matrix.detach().cpu().numpy()

    # Use numpy printoptions to print full array without truncation
    with np.printoptions(threshold=np.inf, linewidth=200, edgeitems=10):
        f.write(f"----- {name} -----\n{matrix}\n\n")

def close_log():
    """
    Close the log file. Should be called at the end of the program.
    """
    global _log_file
    if _log_file:
        _log_file.close()
        _log_file = None

def get_log_filename():
    """
    Return the current log filename.
    """
    return _log_file_name

"""
General repeated used utils here.
"""
import os


def ensure_local_folder(local_folder_name):
    """
    Creates a local folder(including parent directories) if it doesn't exist.
    """
    if not os.path.exists(local_folder_name):
        os.makedirs(local_folder_name)

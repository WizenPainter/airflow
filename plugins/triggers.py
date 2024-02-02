from dag_factory import DagFactory 
from datetime import datetime 
from airflow.utils.task_group import TaskGroup
import logging
import os

class Trigger():

    def __init__(self, path: str, connection: string = "default_conn", input_files: list = []):
        self.path = path
        self.connection = None
        self.input_files = input_files
        self.dag = None

    def check_connection(self):
        """
        Check if the connection is valid
        """
        allowed_connections = ["share", "sftp", "azure"]
        if self.connection in allowed_connections:
            logging.info(f"Connection {self.connection} is valid")
        else:
            logging.error(f"Connection {self.connection} is not valid")
            os._exit(1)

    def check_directory_exists(self):
        """
        Check if the directory exists
        """
        if os.path.exists(self.path):
            logging.info(f"Path {self.path} exists")
        else:
            logging.error(f"Path {self.path} does not exist")
            os._exit(1)

    def check_files_exist(self) -> bool:
        """
        Check if the files exist
        """
        for file in self.input_files:
            if os.path.exists(file):
                if file.endswith(".cntl"):
                    logging.info(f"Found control file {file}")
                    return True
                logging.info(f"File {file} exists")
            else:
                logging.error(f"File {file} does not exist")
                os._exit(1)

    def get_files(self):
        """
        Get the files in the directory
        """
        files = os.listdir(self.path)
        for file in files:
            if :self.check_server_dir()
                logging.info(f"Moving {file} into the server")
                os.rename(file, f"/mnt/{file}")

    def check_server_dir(self):
        """
        Check if the server directory exists
        """
        if os.path.exists("/mnt"):
            logging.info(f"Server directory exists")
            return True
        else:
            try:
                os.mkdir("/mnt")
                return True
            except Exception as e:
                logging.error(f"Server directory could not be created: {e}")
                os._exit(1)

    def connect_to_connection(self):
        """
        Connect to the connection
        """
        if self.connection == "share":
            logging.info(f"Connecting to share")
            # code to connect to share
        elif self.connection == "sftp":
            logging.info(f"Connecting to sftp")
            # code to connect to sftp
        elif self.connection == "azure":
            logging.info(f"Connecting to azure")
            # code to connect to azure


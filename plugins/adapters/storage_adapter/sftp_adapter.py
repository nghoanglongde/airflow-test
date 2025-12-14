# import sys
# from pathlib import Path
# plugins_dir = Path(__file__).parent.parent.parent
# sys.path.insert(0, str(plugins_dir))

# airflow DAG
from adapters.storage_adapter.base_adapter import BaseStorageAdapter
from airflow.providers.sftp.hooks.sftp import SFTPHook
import logging


class SFTPStorageAdapter(BaseStorageAdapter):
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.hook = SFTPHook(ssh_conn_id=conn_id)
        self.logger = logging.getLogger(self.__class__.__name__)

    def list_files(self, path):
        sftp = self.hook.get_conn()
        return sftp.listdir(path)

    def read_file_chunks(self, file_path, chunk_size):
        sftp = self.hook.get_conn()
        with sftp.open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def write_file_chunks(self, file_path: str, chunks):
        sftp = self.hook.get_conn()

        # suppose the source directory is /source/data/file.txt
        # we need to ensure /source/data/ exists in the target SFTP server
        parent_dir = '/'.join(file_path.split('/')[:-1])
        if parent_dir:
            self._create_directory_if_not_exists(sftp, parent_dir)

        total_bytes = 0
        with sftp.open(file_path, 'wb') as f:
            for chunk in chunks:
                f.write(chunk)
                total_bytes += len(chunk)
        return total_bytes

    def _create_directory_if_not_exists(self, sftp, directory):
        try:
            sftp.stat(directory)
        except FileNotFoundError:
            # we need to recursively create parent directories because SFTP has no mkdir -p
            parent_dir = '/'.join(directory.split('/')[:-1])
            if parent_dir and parent_dir != '/':
                self._create_directory_if_not_exists(sftp, parent_dir)

            try:
                sftp.mkdir(directory)
                self.logger.info(f"Created directory: {directory}")
            except IOError:
                try:
                    sftp.stat(directory)
                except FileNotFoundError:
                    raise

    def delete_file(self, file_path):
        sftp = self.hook.get_conn()
        try:
            sftp.remove(file_path)
        except FileNotFoundError:
            pass
    
    def rename_file(self, old_path: str, new_path: str) -> None:
        sftp = self.hook.get_conn()

        try:
            sftp.remove(new_path)
        except FileNotFoundError:
            pass
        sftp.rename(old_path, new_path)


# if __name__ == "__main__":
#     import paramiko

#     SFTP_HOST = "localhost"
#     SFTP_PORT = 2222 
#     SFTP_USERNAME = "sftpuser"
#     SFTP_PASSWORD = "password"

#     class TestingSFTPAdapter(BaseStorageAdapter):
#         def __init__(self, host, port, username, password, key_filename):
#             self.host = host
#             self.port = port
#             self.username = username
#             self.password = password
#             self.key_filename = key_filename
#             self.logger = logging.getLogger(self.__class__.__name__)
#             self._sftp = None
#             self._transport = None

#         def _connect(self):
#             if self._sftp is None:
#                 self._transport = paramiko.Transport((self.host, self.port))
#                 self._transport.connect(username=self.username, password=self.password)
#                 self._sftp = paramiko.SFTPClient.from_transport(self._transport)
#             return self._sftp

#         def close(self):
#             if self._sftp:
#                 self._sftp.close()
#             if self._transport:
#                 self._transport.close()

#         def list_files(self, path):
#             sftp = self._connect()
#             return sftp.listdir(path)

#         def read_file_chunks(self, file_path, chunk_size):
#             sftp = self._connect()
#             with sftp.open(file_path, 'rb') as f:
#                 while True:
#                     chunk = f.read(chunk_size)
#                     if not chunk:
#                         break
#                     yield chunk

#         def write_file_chunks(self, file_path: str, chunks):
#             sftp = self._connect()

#             parent_dir = '/'.join(file_path.split('/')[:-1])
#             if parent_dir:
#                 self._create_directory_if_not_exists(sftp, parent_dir)

#             total_bytes = 0
#             with sftp.open(file_path, 'wb') as f:
#                 for chunk in chunks:
#                     f.write(chunk)
#                     total_bytes += len(chunk)
#             return total_bytes

#         def _create_directory_if_not_exists(self, sftp, directory):
#             try:
#                 sftp.stat(directory)
#             except FileNotFoundError:
#                 sftp.mkdir(directory)
#                 self.logger.info(f"Created directory: {directory}")

#         def delete_file(self, file_path):
#             sftp = self._connect()
#             try:
#                 sftp.remove(file_path)
#             except FileNotFoundError:
#                 pass

#     adapter = TestingSFTPAdapter(
#         host=SFTP_HOST,
#         port=SFTP_PORT,
#         username=SFTP_USERNAME,
#         password=SFTP_PASSWORD,
#     )
    
#     try:
#         test_path = "/data"
#         files = adapter.list_files(test_path)
#         print(f"files in {test_path}: {files}")
#     except Exception as e:
#         print(f"error listing files: {e}")
    
#     try:
#         test_file_path = "/data/test_file.txt"
#         test_content = [b"Hello, ", b"this is ", b"a test file!"]
#         bytes_written = adapter.write_file_chunks(test_file_path, test_content)
#         print(f"successfully wrote to {test_file_path}")
#     except Exception as e:
#         print(f"error writing file: {e}")
    
#     try:
#         test_file_path = "/data/test_file.txt"
#         chunk_size = 1024
#         content = b""
#         for chunk in adapter.read_file_chunks(test_file_path, chunk_size):
#             content += chunk
#     except Exception as e:
#         print(f"error reading file: {e}")
    
#     try:
#         test_file_path = "/data/test_file.txt"
#         adapter.delete_file(test_file_path)
#         print(f"successfully deleted {test_file_path}")
#     except Exception as e:
#         print(f"error deleting file: {e}")
    
#     print("all tests completed")
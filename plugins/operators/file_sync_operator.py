# airflow DAG
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from adapters.storage_adapter.factory import StorageAdapterFactory
import logging
from datetime import datetime
import os
import hashlib


class FileSyncOperator(BaseOperator):
    template_fields = ['source_path']

    def __init__(
        self, source_type, target_type, source_conn_id, target_conn_id,
        source_path, modulo_id, num_batches, chunk_size = 10 * 1024 * 1024,  # 10MB default
        transformation_func = None, **kwargs):
        super().__init__(**kwargs)
        self.source_type = source_type
        self.target_type = target_type
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.source_path = source_path
        self.target_path = source_path  # target path same as source path cause the test requires it
        self.modulo_id = modulo_id
        self.num_batches = num_batches
        self.chunk_size = chunk_size
        self.transformation_func = transformation_func
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, context):
        source_adapter = StorageAdapterFactory.create_adapter(
            self.source_type,
            self.source_conn_id
        )
        target_adapter = StorageAdapterFactory.create_adapter(
            self.target_type,
            self.target_conn_id
        )

        # Get file list and filter by modulo
        all_files, actual_source_path = self._get_file_list(source_adapter)
        source_files = self._filter_files_by_modulo(all_files)

        checkpoint = self._load_checkpoint(context)
        stats = {
            'total_files': len(source_files),
            'synced': 0,
            'skipped': 0,
            'failed': 0,
            'total_bytes': 0
        }

        # handle empty file list
        if not source_files:
            self.logger.info("No files to sync in this batch")
            self._clear_checkpoint(context)
            return stats

        for file_name in source_files:
            try:
                # check checkpoint => we skip if already synced
                if self._is_file_synced(checkpoint, file_name):
                    stats['skipped'] += 1
                    continue

                source_file = f"{actual_source_path}/{file_name}"
                target_file = f"{self.target_path}/{file_name}"
                temp_file = f"{target_file}.tmp"

                bytes_transferred = self._transfer_file(
                    source_adapter,
                    target_adapter,
                    source_file,
                    temp_file,
                    target_file
                )
                self._mark_file_synced(checkpoint, file_name, bytes_transferred)
                stats['synced'] += 1
                stats['total_bytes'] += bytes_transferred

                self.logger.info(f"successfully synced {file_name} ({bytes_transferred} bytes)")

            except Exception as e:
                self.logger.error(f"failed to sync {file_name}: {str(e)}")
                stats['failed'] += 1

                # cleanup/rollback
                self._cleanup_failed_transfer(target_adapter, temp_file, target_file)
                self._save_checkpoint(context, checkpoint)

                raise AirflowException(f"file sync failed at '{file_name}': {str(e)}\n")

        # if all files synced, clear checkpoint for rerun when needed
        self._clear_checkpoint(context)
        self.logger.info(f"all files synced successfully: {stats}")
        return stats

    def _transfer_file(self, source_adapter, target_adapter, source_file, temp_file, target_file):
        chunks = source_adapter.read_file_chunks(source_file, self.chunk_size)

        # apply transformation if provided
        if self.transformation_func:
            chunks = (self.transformation_func(chunk) for chunk in chunks)

        # write chunks to temp file
        total_bytes = target_adapter.write_file_chunks(temp_file, chunks)

        # if all chunks successful => we rename temp file to target file in SFTP target server
        target_adapter.rename_file(temp_file, target_file)

        return total_bytes

    def _cleanup_failed_transfer(self, target_adapter, temp_file, target_file):
        try:
            target_adapter.delete_file(temp_file)
            target_adapter.delete_file(target_file)
        except Exception as e:
            self.logger.error(f"error during cleanup: {str(e)}")

    def _load_checkpoint(self, context):
        # We will use XCom to store checkpoint for each DAG run separately
        task_instance = context['task_instance']
        dag_run_id = context['dag_run'].run_id

        checkpoint = task_instance.xcom_pull(
            task_ids=self.task_id,
            key=f'file_sync_checkpoint_{dag_run_id}'
        )
        return checkpoint if checkpoint else {}

    def _save_checkpoint(self, context, checkpoint):
        task_instance = context['task_instance']
        dag_run_id = context['dag_run'].run_id

        task_instance.xcom_push(
            key=f'file_sync_checkpoint_{dag_run_id}',
            value=checkpoint
        )

    def _clear_checkpoint(self, context):
        task_instance = context['task_instance']
        dag_run_id = context['dag_run'].run_id

        task_instance.xcom_push(
            key=f'file_sync_checkpoint_{dag_run_id}',
            value=None
        )

    def _is_file_synced(self, checkpoint, file_name):
        return file_name in checkpoint

    def _mark_file_synced(self, checkpoint, file_name, bytes_transferred):
        checkpoint[file_name] = {
            'synced_at': datetime.utcnow().isoformat(),
            'bytes': bytes_transferred
        }

    def _get_file_list(self, source_adapter):
        is_dir = source_adapter.is_directory(self.source_path)

        if is_dir:
            try:
                files = source_adapter.list_files(self.source_path)
                return files, self.source_path
            except Exception as e:
                raise AirflowException(f"failed to list files from {self.source_path}")
        else:
            # single file case
            file_name = os.path.basename(self.source_path)
            directory_path = os.path.dirname(self.source_path)
            return [file_name], directory_path

    def _filter_files_by_modulo(self, files):
        # so I will use modulo for splitting files into batches
        if self.num_batches <= 0:
            raise AirflowException(f"num_batches must be positive")

        filtered_files = []
        for file_name in files:
            # hash the filename using MD5 and convert to integer
            hash_hex = hashlib.md5(file_name.encode()).hexdigest()
            hash_int = int(hash_hex, 16)
            modulo_value = abs(hash_int) % self.num_batches

            if modulo_value == self.modulo_id:
                filtered_files.append(file_name)
        self.logger.info(f"{len(filtered_files)}/{len(files)} files assigned to this batch")
        return filtered_files

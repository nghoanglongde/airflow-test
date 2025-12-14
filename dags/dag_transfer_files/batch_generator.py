# airflow DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
import logging
import os

logger = logging.getLogger(__name__)

def list_files_from_directory(sftp, directory_path):
    files = sftp.listdir(directory_path)
    files.sort()
    return files


def get_file_from_path(file_path):
    return [os.path.basename(file_path)]


def generate_batches_from_directory(conn_id, directory_path, num_batches: 3):
    # always create num_batches batches (even if empty)
    batches = [{'batch_id': i, 'files': []} for i in range(num_batches)]

    try:
        hook = SFTPHook(ssh_conn_id=conn_id)
        sftp = hook.get_conn()
        files = list_files_from_directory(sftp, directory_path)

        # Distribute files using modulo
        for idx, file in enumerate(files):
            batch_idx = idx % num_batches
            batches[batch_idx]['files'].append(file)
    except Exception as e:
        logger.warning(f"could not list files from {directory_path}")

    return {
        'source_type': 'sftp',
        'target_type': 'sftp',
        'source_conn_id': conn_id,
        'path': directory_path,
        'chunk_size': 10 * 1024 * 1024,
        'batches': batches
    }


def generate_batches_from_file(conn_id, file_path, num_batches=3):
    logger.info(f"Processing single file: {file_path}")

    batches = [{'batch_id': i, 'files': []} for i in range(num_batches)]

    files = get_file_from_path(file_path)
    batches[0]['files'] = files

    directory_path = '/'.join(file_path.split('/')[:-1])

    return {
        'source_type': 'sftp',
        'target_type': 'sftp',
        'source_conn_id': conn_id,
        'path': directory_path,
        'chunk_size': 10 * 1024 * 1024,
        'batches': batches
    }

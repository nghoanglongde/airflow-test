# airflow DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
import logging
import os


def list_files_from_directory(sftp, directory_path):
    files = sftp.listdir(directory_path)
    files.sort()
    return files


def get_file_from_path(file_path):
    return [os.path.basename(file_path)]


def generate_batches_from_directory(conn_id, directory_path, num_batches: 3):
    logger = logging.getLogger(__name__)
    hook = SFTPHook(ssh_conn_id=conn_id)
    sftp = hook.get_conn()

    try:
        files = list_files_from_directory(sftp, directory_path)
    except Exception as e:
        return {
            'source_type': 'sftp',
            'target_type': 'sftp',
            'source_conn_id': conn_id,
            'path': directory_path,
            'chunk_size': 10 * 1024 * 1024,
            'batches': []
        }

    batches = [{'batch_id': i, 'files': []} for i in range(num_batches)]

    # distribute files using modulo
    for idx, file in enumerate(files):
        batch_idx = idx % num_batches
        batches[batch_idx]['files'].append(file)

    return {
        'source_type': 'sftp',
        'target_type': 'sftp',
        'source_conn_id': conn_id,
        'path': directory_path,
        'chunk_size': 10 * 1024 * 1024,
        'batches': batches
    }


def generate_batches_from_file(conn_id, file_path):
    logger = logging.getLogger(__name__)
    logger.info(f"Processing single file: {file_path}")

    files = get_file_from_path(file_path)
    directory_path = '/'.join(file_path.split('/')[:-1])
    batches = [{'batch_id': 0, 'files': files}]

    return {
        'source_type': 'sftp',
        'target_type': 'sftp',
        'source_conn_id': conn_id,
        'path': directory_path,
        'chunk_size': 10 * 1024 * 1024,
        'batches': batches
    }

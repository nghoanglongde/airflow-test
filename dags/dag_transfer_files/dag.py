# airflow DAG
from jinja2 import Template
from yaml import load, Loader
import os, inspect
from dag_builder import DagBuilder
from datetime import datetime

# script directory
current_path = os.path.dirname(
	os.path.abspath(
		inspect.getfile(
			inspect.currentframe()
		)
	)
)

# config for directory sync testing
# config = {
#     'source_type': 'sftp',
#     'target_type': 'sftp',
#     'source_conn_id': 'source_sftp_conn',
#     'target_conn_id': 'target_sftp_conn',
#     'source_path': "/data/source/2025-12-12",
#     'target_path': "/data/source/2025-12-12",
#     'num_batches': 3,
#     'chunk_size': 10 * 1024 * 1024
# }

# config for single file sync testing
# config = {
#     'source_type': 'sftp',
#     'target_type': 'sftp',
#     'source_conn_id': 'source_sftp_conn',
#     'target_conn_id': 'target_sftp_conn',
#     'source_path': f"/data/source/2025-12-12/file_001.txt",
#     'target_path': f"/data/source/2025-12-12",
#     'num_batches': 3,
#     'chunk_size': 10 * 1024 * 1024
# }

# config for directory sync daily
config = {
    'source_type': 'sftp',
    'target_type': 'sftp',
    'source_conn_id': 'source_sftp_conn',
    'target_conn_id': 'target_sftp_conn',
    'source_path': "/data/source/{{macros.caketest.local_ds(ts)}}",
    'target_path': "/data/source/{{macros.caketest.local_ds(ts)}}",
    'num_batches': 3,
    'chunk_size': 10 * 1024 * 1024
}

with open(os.path.join(current_path, 'config.yaml'), 'r') as infile:
    template = Template(infile.read())

rendered_config = template.render(config=config)
configs = load(rendered_config, Loader=Loader)
dag = DagBuilder(configs).build()
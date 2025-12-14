# airflow DAG
from jinja2 import Template
from yaml import load, Loader
import os, inspect
from dag_builder import DagBuilder
from dag_transfer_files.batch_generator import generate_batches_from_directory, generate_batches_from_file
from datetime import datetime

# script directory
current_path = os.path.dirname(
	os.path.abspath(
		inspect.getfile(
			inspect.currentframe()
		)
	)
)

execution_date = datetime.now().strftime('%Y-%m-%d')


# batch_config = generate_batches_from_directory(
#     conn_id='source_sftp_conn',
#     directory_path=f"/data/source/2025-12-13",
#     num_batches=3
# )

# batch_config = generate_batches_from_file(
#     conn_id='source_sftp_conn',
#     file_path=f"/data/source/2025-12-11/file_001.txt"
# )

batch_config = generate_batches_from_directory(
    conn_id='source_sftp_conn',
    directory_path=f"/data/source/{execution_date}",
    num_batches=3
)

with open(os.path.join(current_path, 'config.yaml'), 'r') as infile:
    template = Template(infile.read())

rendered_config = template.render(config=batch_config)
configs = load(rendered_config, Loader=Loader)
dag = DagBuilder(configs).build()
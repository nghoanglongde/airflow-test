# airflow DAG
from jinja2 import Template
from yaml import load, Loader
import pprint, os, inspect
from dag_builder import DagBuilder

pp = pprint.PrettyPrinter(indent=2)

current_path = os.path.dirname(
	os.path.abspath(
		inspect.getfile(
			inspect.currentframe()
		)
	)
)

with open(os.path.join(current_path, 'config.yaml'), 'r') as infile:
    template = Template(infile.read())

configs = load(template.render(), Loader=Loader)


dag = DagBuilder(configs).build()
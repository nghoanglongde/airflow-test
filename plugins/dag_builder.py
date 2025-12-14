import airflow
import logging


class DagBuilder():
    """Generate airflow.model.DAG from config"""

    def __init__(self, configs):
        self.configs = configs

    def build(self):
        dag_id = self.configs['DAG']['dag_id']
        self.logger = logging.getLogger(dag_id)

        dag_kwargs = self.configs['DAG']['args']
        dag_kwargs['dag_id'] = dag_id
        dag = airflow.models.DAG(**dag_kwargs)
        with dag:
            tasks = {}
            cfg_all_tasks = self.configs['DAG']['tasks']
            # build tasks
            for task_id in cfg_all_tasks:
                operator = cfg_all_tasks[task_id]['operator']
                if 'args' in cfg_all_tasks[task_id]:
                    task_kwargs = cfg_all_tasks[task_id]['args']
                else:
                    task_kwargs = {}
                task_kwargs['task_id'] = task_id
                tasks[task_id] = operator(**task_kwargs)
            # set upstream for each task
            for task_id in cfg_all_tasks:
                cfg = cfg_all_tasks[task_id]
                if 'upstream' in cfg:
                    upstream = [
                        tasks[ups] for ups in cfg['upstream']
                    ]
                    tasks[task_id].set_upstream(upstream)
        return dag
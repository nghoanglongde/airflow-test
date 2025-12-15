"""This is the class you derive to create a plugin."""
import pendulum
from airflow.plugins_manager import AirflowPlugin


TIMEZONE = pendulum.timezone('Asia/Ho_Chi_Minh')

def local_ds(ts, next_date=False):
    if next_date:
        local_execution_date = TIMEZONE.convert(
            pendulum.parse(ts)
        ).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).add(days=1)
    else:
        local_execution_date = TIMEZONE.convert(
            pendulum.parse(ts)
        ).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    return local_execution_date.strftime('%Y-%m-%d')

class CakeTestPlugin(AirflowPlugin):
    name = "caketest"
    macros = [
        local_ds
    ]

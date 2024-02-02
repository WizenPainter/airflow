"""
This file holds the DAG Factory
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

class DAGFactory:
    """
    Class that provides a useful method to build an Airflow DAG
    """

    @classmethod
    def create_dag(cls, dagname: str, default_args: dict = {}, catchup: bool = False, concurrency: int = 5, cron: str = None):
        """
        params: 
        """
        DEFAULT_ARGS = {
            'owner': 'Jaime',
            'depends_on_past': False,
            'start_date': datetime(2024, 02, 02),
            'email': ['jaime.guzman@norteanalytics.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
        
        DEFAULT_ARGS.update(default_args)
        dagargs = {
            'default_args': DEFAULT_ARGS,
            'schedule_interval': cron,
            'catchup': catchup,
            'concurrency': concurrency
        }

        dag = DAG(dagname, **dagargs)
        return dag

    @classmethod
    def add_tasks_to_dag(cls, dag: DAG, tasks: dict) -> DAG:
        """
        adds tasks to the dag
        params:
          dag: DAG object
          tasks(dict): dictionary with the tasks to be added to the dag
        """
        with dag as dag:
            aux_dict = {}

            for func in tasks:
                task_id = func.__name__
                task = PythonOperator(
                    task_id=task_id,
                    python_callable=func,
                    dag=dag
                )
                aux_dict[task_id] = task

            for func, dependencies in tasks.items():
                task_id = func.__name__
                for dependency in dependencies:
                    aux_dict[dependency.__name__] >> aux_dict[task_id]
        
        return dag

    @classmethod
    def get_airflow_dag(cls, dagname, tasks, default_args: dict ={}, catchup: bool = False, concurrency: int = 5, cron: str = None):
        """
        Method used to create the dags
        """
        dag = cls.create_dag(dagname, default_args, catchup, concurrency, cron)
        dag = cls.add_tasks_to_dag(dag, tasks)
        return dag

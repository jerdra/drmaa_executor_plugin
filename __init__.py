from typing import Any, List, Type
from airflow.plugins_manager import AirflowPlugin
from drmaa_executor_plugin.executors.drmaa_executor import DRMAAV1Executor

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.executors.base_executor import BaseExecutor


class drmaa_executor_plugin(AirflowPlugin):
    name = "drmaa_executor_plugin"
    executors: List[Type[BaseExecutor]] = [DRMAAV1Executor]
    hooks: List[BaseHook] = []
    operators: List[BaseOperator] = []
    macros: List[Any] = []

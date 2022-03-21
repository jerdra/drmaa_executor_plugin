from setuptools import setup

setup(name="my-package",
      entry_points={
          "airflow.plugins":
          ["drmaa_executors = drmaa_executor_plugin:DRMAAExecutorPlugin"]
      })

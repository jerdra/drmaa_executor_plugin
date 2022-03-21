from setuptools import setup

if __name__ == '__main__':
    setup(name="drmaa_executor_plugin",
          entry_points={
              "airflow.plugins":
              ["drmaa_executors = drmaa_executor_plugin:DRMAAExecutorPlugin"]
          })

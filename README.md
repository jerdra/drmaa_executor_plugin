# Airflow Plugin - DRMAA

This plugin enables Airflow to use DRMAA-compliant HPC schedulers as an Executor for jobs

# Executors

## DRMAAV1Executor

Implementation for using drmaa v1 API for interfacing with compliant schedulers

## HybridDRMAAExecutor

Allow Airflow scheduler to submit jobs to run jobs locally (LocalExecutor) or on drmaa v1 compliant schedulers


# Requirements

Using `drmaa_executor_plugin` requires the user to set up the [drmaa](https://github.com/pygridtools/drmaa-python) python package.

```
pip install drmaa
EXPORT DRMAA_LIBRARY_PATH=/path/to/libdrmaa.so.x
```

# Using with Airflow

In `airflow.cfg` define:

```
[core]

# Airflow < 2.0
executor = drmaa_executors.DRMAAV1Executor

# Airflow >= 2.0
# See:
# https://github.com/apache/airflow/blob/2.0.0/UPDATING.md#custom-executors-is-loaded-using-full-import-path

executor = drmaa_executor_plugin.executors.drmaa_executor.DRMAAV1Executor

```

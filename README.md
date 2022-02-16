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

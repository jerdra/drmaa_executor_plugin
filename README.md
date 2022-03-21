# Airflow Plugin - DRMAA

This plugin enables Airflow to use DRMAA-compliant HPC schedulers as an Executor for jobs

# Executors

## DRMAAV1Executor

Implementation for using drmaa v1 API for interfacing with compliant schedulers

## HybridDRMAAExecutor

Allow Airflow scheduler to submit jobs to run jobs locally (LocalExecutor) or on drmaa v1 compliant schedulers


# Installation

The package can be installed using pip:

```
git clone https://github.com/jerdra/drmaa_executor_plugin.git
cd drmaa_executor_plugin
pip install -r requirements.txt
pip install .
```

Additionally, using the `drmaa_executor_plugin` requires the user to set up the [drmaa](https://github.com/pygridtools/drmaa-python) python package; `DRMAA_LIBRARY_PATH` must point to the user's `libdrmaa.so` file.

```
EXPORT DRMAA_LIBRARY_PATH=/path/to/libdrmaa.so.x
```

# Using with Airflow

Once installed, the executor can be used by updating `airflow.cfg`'s `[core]` section:

```
[core]
...

# Airflow < 2.0
executor = drmaa_executors.DRMAAV1Executor

# Airflow >= 2.0
# See:
# https://github.com/apache/airflow/blob/2.0.0/UPDATING.md#custom-executors-is-loaded-using-full-import-path
executor = drmaa_executor_plugin.executors.DRMAAV1Executor

```

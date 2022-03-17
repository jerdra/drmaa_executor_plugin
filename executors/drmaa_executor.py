from __future__ import annotations
import drmaa  # type: ignore
from drmaa_executor_plugin.drmaa_patches import PatchedSession as drmaaSession

from typing import (TYPE_CHECKING, Optional, Generator, Callable, TypeVar)

from functools import wraps

from airflow.executors.base_executor import BaseExecutor, NOT_STARTED_MESSAGE
from airflow.exceptions import AirflowException

import drmaa_executor_plugin.config_adapters as adapters
import drmaa_executor_plugin.stores as drmaa_stores

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstanceKey

if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from drmaa_executor_plugin.stores import JobStoreType, JobID

JOB_STATE_MAP = {
    drmaa.JobState.QUEUED_ACTIVE: State.QUEUED,
    drmaa.JobState.RUNNING: State.RUNNING,
    drmaa.JobState.DONE: State.SUCCESS,
    drmaa.JobState.FAILED: State.FAILED
}

# How to handle API breaking changes with typed dict?

T = TypeVar("T")


def check_started(method: Callable[..., T]) -> Callable[..., T]:
    '''
    Check whether executor has been initialized with
    drmaaSession object
    '''
    @wraps(method)
    def _impl(self, *method_args, **method_kwargs):
        if self.session is None or self.store is None:
            raise AirflowException(NOT_STARTED_MESSAGE)

        return method(self, *method_args, **method_kwargs)

    return _impl


class DRMAAV1Executor(BaseExecutor, LoggingMixin):
    """
    Submit jobs to an HPC cluster using the DRMAA v1 API
    """

    drmaa_section = 'drmaa'

    def __init__(self,
                 max_concurrent_jobs: Optional[int] = None,
                 parallelism: int = 0):
        super().__init__(parallelism=parallelism)

        self.jobs_submitted: int = 0
        self.session: Optional[drmaaSession] = None
        self.store: Optional[JobStoreType] = None

        # Not yet implemented
        self.max_concurrent_jobs: Optional[int] = max_concurrent_jobs

    def iter_scheduled_jobs(
            self) -> Generator[tuple[JobID, TaskInstanceKey], None, None]:
        '''
        Iterate over scheduled jobs
        '''
        for job_id, instance_key in self.store.get_or_create().items():
            yield job_id, instance_key

    @property
    def active_jobs(self) -> int:
        return len(self.store.get_or_create().keys())

    def start(self) -> None:
        self.log.info("Initializing DRMAA session")

        if self.session is None:
            self.session = drmaaSession()
            self.session.initialize()

        if self.store is None:
            self.log.info("Initializing backend store for job tracking")
            drmaa_config = conf.as_dict(
                display_sensitive=True)[self.drmaa_section]
            self.store = drmaa_stores.get_store(
                drmaa_config.get("store", "VariableStore"),
                drmaa_config.get("store_metadata", {}))

        self.log.info(
            "Getting job tracking Airflow Variable: `scheduler_job_ids`")
        current_jobs = self.store.get_or_create()

        if current_jobs:
            print_jobs = "\n".join([f"{j_id}" for j_id in current_jobs])
            self.log.info(f"Jobs from previous session:\n{print_jobs}")
        else:
            self.log.info("No jobs are currently being tracked")

    @check_started
    def end(self) -> None:
        self.log.info("Cleaning up remaining job statuses")
        self.sync()

        self.log.info("Terminating DRMAA session")
        self.session.exit()

    @check_started
    def sync(self) -> None:
        """
        Called periodically by `airflow.executors.base_executor.BaseExecutor`'s
        heartbeat.

        Read the current state of tasks in the scheduler and update the metaDB
        """

        # Go through currently running jobs and update state
        for job_id, task_instance_key in self.iter_scheduled_jobs():
            drmaa_status = self.session.jobStatus(job_id)
            try:
                status = JOB_STATE_MAP[drmaa_status]
            except KeyError:
                self.log.info(
                    "Got unexpected state {drmaa_status} for job #{job_id}"
                    " Cannot be mapped into an Airflow TaskInstance State"
                    " Will try again in next sync attempt...")
            else:
                # Need taskinstancekey
                self.change_state(task_instance_key, status)
                self.store.drop_job(job_id)

    @check_started
    def execute_async(self,
                      key: TaskInstanceKey,
                      command: CommandType,
                      executor_config: adapters.DRMAACompatible,
                      queue: Optional[str] = None) -> None:
        '''
        Submit slurm job and track job id
        '''

        self.log.info(f"Submitting job {key} with command {command} with"
                      f" configuration options:\n{executor_config})")
        jt = executor_config.get_drmaa_config(self.session.createJobTemplate())

        # CommandType always begins with "airflow" binary command
        jt.remoteCommand = command[0]

        # args to airflow follow
        jt.args = command[1:]

        # TODO: Figure out exception handling when job submission fails
        job_id = self.session.runJob(jt)

        self.log.info(f"Submitted Job {job_id}")
        self.store.add_job(job_id, key)

        # Prevent memory leaks on C back-end, running jobs unaffected
        # https://drmaa-python.readthedocs.io/en/latest/drmaa.html
        self.session.deleteJobTemplate(jt)
        self.jobs_submitted += 1

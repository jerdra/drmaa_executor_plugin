from __future__ import annotations
import drmaa
from drmaa_executor_plugin.drmaa_patches import PatchedSession as drmaaSession

from typing import TYPE_CHECKING, Optional, TypedDict, Generator, Dict, cast

from functools import wraps

from airflow.executors.base_executor import BaseExecutor, NOT_STARTED_MESSAGE
from airflow.exceptions import AirflowException

import drmaa_executor_plugin.config_adapters as adapters
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.models import Variable

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.executors.base_executor import CommandType
    from sqlalchemy.orm import Session

JOB_STATE_MAP = {
    drmaa.JobState.QUEUED_ACTIVE: State.QUEUED,
    drmaa.JobState.RUNNING: State.RUNNING,
    drmaa.JobState.DONE: State.SUCCESS,
    drmaa.JobState.FAILED: State.FAILED
}

JobID = int
_TaskInstanceKeyDict = TypedDict('_TaskInstanceKeyDict', {
    'dag_id': str,
    'task_id': str,
    'run_id': str,
    'try_number': int
})

JobTrackingType = Dict[JobID, _TaskInstanceKeyDict]


def check_started(method):
    '''
    Check whether executor has been initialized with
    drmaa.Session object
    '''
    @wraps(method)
    def _impl(self, *method_args, **method_kwargs):
        if self.session is None:
            raise AirflowException(NOT_STARTED_MESSAGE)
        method(self, *method_args, **method_kwargs)

    return _impl


# TODO: Create JobTracker helper class
class DRMAAV1Executor(BaseExecutor, LoggingMixin):
    """
    Submit jobs to an HPC cluster using the DRMAA v1 API
    """
    def __init__(self, max_concurrent_jobs: Optional[int] = None):
        super().__init__()

        self.active_jobs: int = 0
        self.jobs_submitted: int = 0
        self.session: Optional[drmaa.Session] = None

        # Not yet implemented
        self.max_concurrent_jobs: Optional[int] = max_concurrent_jobs

    def iter_scheduled_jobs(
            self) -> Generator[tuple[JobID, TaskInstanceKey], None, None]:
        '''
        Iterate over scheduled jobs
        '''
        for job_id, instance_info in self._get_or_create_job_ids().items():
            yield job_id, TaskInstanceKey(**instance_info)

    @property
    def active_jobs(self) -> int:
        return len(self._get_or_create_job_ids())

    @active_jobs.setter
    def active_jobs(self, val):
        self.active_jobs = val

    # TODO: Make `scheduler_job_ids` configurable under [executor]
    @provide_session
    def _get_or_create_job_ids(self,
                               session: Optional[Session] = None
                               ) -> JobTrackingType:

        current_jobs = Variable.get("scheduler_job_ids",
                                    default_var=None,
                                    deserialize_json=True)
        if not current_jobs:
            current_jobs = {}
            self.log.info("Setting up job tracking Airflow variable...")
            Variable.set("scheduler_job_ids",
                         current_jobs,
                         description="Scheduler Job ID tracking",
                         serialize_json=True,
                         session=session)
            self.log.info("Created `scheduler_job_ids` variable")

        return current_jobs

    @provide_session
    def _update_job_tracker(self,
                            jobs: JobTrackingType,
                            session: Optional[Session] = None) -> None:
        Variable.update("scheduler_job_ids",
                        jobs,
                        serialize_json=True,
                        session=session)

    def _drop_from_tracking(self, job_id: JobID) -> None:
        self.log.info(
            f"Removing Job {job_id} from tracking variable `scheduler_job_ids`"
        )

        jobs = self._get_or_create_job_ids()
        try:
            jobs.pop(job_id)
        except KeyError:
            self.log.error(f"Failed to remove {job_id}, job was not"
                           " being tracked by Airflow!")
        else:
            self._update_job_tracker(jobs)
            self.log.info(
                f"Successfully removed {job_id} from `scheduler_job_ids`")

    def _push_to_tracking(self, job_id: JobID, key: TaskInstanceKey) -> None:
        self.log.info(
            "Adding Job {job_id} to tracking variable `scheduler_job_ids`")

        # Convert TaskInstanceKey to serializable form
        key_dict = _taskkey_to_dict(key)
        entry = {job_id: key_dict}

        current_jobs = self._get_or_create_job_ids()
        current_jobs.update(entry)
        self._update_job_tracker(current_jobs)
        self.log.info("Successfully added {job_id} to `scheduler_job_ids`")

    def start(self) -> None:
        self.log.info("Initializing DRMAA session")
        self.session = drmaaSession()
        self.session.initialize()

        self.log.info(
            "Getting job tracking Airflow Variable: `scheduler_job_ids`")
        current_jobs = self._get_or_create_job_ids()

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
                self._drop_from_tracking(job_id)

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
        self._push_to_tracking(job_id, key)

        # Prevent memory leaks on C back-end, running jobs unaffected
        # https://drmaa-python.readthedocs.io/en/latest/drmaa.html
        self.session.deleteJobTemplate(jt)
        self.jobs_submitted += 1


def _taskkey_to_dict(key: TaskInstanceKey) -> _TaskInstanceKeyDict:
    return {
        "dag_id": key.dag_id,
        "task_id": key.task_id,
        "run_id": key.run_id,
        "try_number": key.try_number
    }

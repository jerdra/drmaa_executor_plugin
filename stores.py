'''
Back-end stores for tracking persistent active job state
for DRMAA Executor
'''
from __future__ import annotations

from typing import TYPE_CHECKING, cast, Dict, Optional
from typing_extensions import NotRequired, TypedDict

from abc import abstractmethod, ABC

from airflow.models import Variable
from airflow.utils.session import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from sqlalchemy.orm import Session

_TaskInstanceKeyDict = TypedDict(
    '_TaskInstanceKeyDict', {
        'dag_id': str,
        'task_id': str,
        'run_id': NotRequired[Optional[str]],
        'execution_date': NotRequired[Optional[str]],
        'try_number': int
    })

JobID = int
JobTrackingType = Dict[JobID, _TaskInstanceKeyDict]


class DRMAAJobStoreMixin(ABC):
    '''
    Interface for defining a store
    backend for keeping track of DRMAA jobs
    persistently across sessions
    '''

    @abstractmethod
    def get_or_create(self, session: Session = None):
        raise NotImplementedError

    @abstractmethod
    def drop_job(self, job: JobID):
        raise NotImplementedError

    @abstractmethod
    def add_job(self, job: JobID, key: TaskInstanceKey):
        raise NotImplementedError


# TODO: Do an airflow version check for behaviour implementation
class VariableStore(DRMAAJobStoreMixin, LoggingMixin):
    '''
    Use Airflow Variable as a back end job store
    using a set key
    '''
    def __init__(self, metadata={}):
        self.key: str = metadata.get('key', 'scheduler_job_ids')
        self.description: str = metadata.get(
            'description', 'DRMAA Scheduler Job ID tracking')
        self.value = self.get_or_create()

    @provide_session
    def get_or_create(self, session: Session = None) -> JobTrackingType:
        value = Variable.get(self.key, default_var=None, deserialize_json=True)
        if value is None:
            value = {}
            self.log.info("Setting up job tracking Airflow variable...")

            # Cannot use Variable.set in Airflow < 2.2
            if session is not None:
                session.add(
                    Variable(
                        key=self.key,
                        val="{}",
                        description=self.description,
                    ))
                session.flush()
            self.log.info(f"Created `{self.key}` variable")

        return value

    def add_job(self, job_id: JobID, key: TaskInstanceKey):
        key_dict = _taskkey_to_dict(key)
        entry = {job_id: key_dict}
        self._update(entry)
        self.log.info(f"Successfully added {job_id} to `{self.key}`")

    @provide_session
    def _update(self, jobs: JobTrackingType, session: Session = None):
        value = self.get_or_create()
        value.update(jobs)
        Variable.update(self.key, jobs, serialize_json=True, session=session)

    def drop_job(self, job_id: JobID):
        value = self.get_or_create()
        try:
            job_details = value.pop(job_id)
        except KeyError:
            self.log.error(f"Failed to remove {job_id}, job was not"
                           " being tracked by Airflow!")
        else:
            self._update(value)
            to_print = "\n".join([f"{k}: {v}" for k, v in job_details.items()])
            self.log.info(
                f"Successfully removed {job_id} from `{self.key}` with"
                " the following Task information:\n"
                f"{to_print}")


STORE_REGISTER = {"VariableStore": VariableStore}
JobStoreType = DRMAAJobStoreMixin


def get_store(store_name: str, metadata={}) -> JobStoreType:
    '''
    Retrieve a registered store and configure it
    '''

    return STORE_REGISTER[store_name](metadata)


def _taskkey_to_dict(key: TaskInstanceKey) -> _TaskInstanceKeyDict:
    return cast(_TaskInstanceKeyDict, key._asdict())

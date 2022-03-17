"""
Tests for DRMAA Executor implementation. DRMAA calls will be mocked
"""

from __future__ import annotations
from unittest.mock import MagicMock
import pytest
from copy import deepcopy

from drmaa_executor_plugin.executors.drmaa_executor import DRMAAV1Executor
from drmaa_executor_plugin.drmaa_patches import PatchedSession
from drmaa_executor_plugin.config_adapters import DRMAAConfig
from drmaa_executor_plugin.stores import DRMAAJobStoreMixin

from airflow.models.taskinstance import TaskInstanceKey
from airflow.exceptions import AirflowException


class MockJobStore(DRMAAJobStoreMixin):
    '''
    Mock dictionary-based implementation of a DRMAAJobStore backend
    '''
    def __init__(self, metadata={}):
        self.value = None
        self.dropped_jobs = []
        self.added_jobs = []

        self.value = self.get_or_create()

    def get_or_create(self):
        if self.value is None:
            self.value = {}

        # deepcopy is needed since drop/add modifies dictionary
        return deepcopy(self.value)

    def drop_job(self, job_id):
        try:
            self.value.pop(job_id)
        except KeyError:
            pass
        self.dropped_jobs.append(job_id)

    def add_job(self, job_id, key):
        self.value[job_id] = key
        self.added_jobs.append((job_id, key))


@pytest.fixture
def executor():
    '''
    Provide a sessioned executor
    '''

    executor = DRMAAV1Executor()
    session = PatchedSession()
    executor.session = session
    return executor


@pytest.fixture
def task_key():
    '''
    Provide configured TaskInstanceKey
    '''
    return TaskInstanceKey(dag_id=1,
                           task_id=1,
                           execution_date="9999-99-99 99:99:99.999999",
                           try_number=1)


def test_executor_fails_when_no_drmaa_session_set():
    '''
    If no DRMAA session is provided an exception should be raised
    suggesting that the executor must be started with .start()
    '''

    executor = DRMAAV1Executor()

    with pytest.raises(AirflowException):
        executor.execute_async()


def test_job_submission_attempts_to_store_correct_information(
        executor, task_key):
    '''
    execute_async() should create an Airflow Variable
    tracking:
        - The current job ID
        - A task instance key (dag_id, task_id, execution_date, try_number)
        - Command is set appropriately
    '''

    MOCK_JOB_KEY = 12345
    executor.session.runJob = MagicMock(return_value=MOCK_JOB_KEY)
    executor.store = MockJobStore()
    executor_config = DRMAAConfig()
    executor.start()

    command = "airflow TESTING"

    executor.execute_async(task_key, command, executor_config)
    expected_stored_dict = {MOCK_JOB_KEY: task_key}

    assert executor.active_jobs == 1
    assert len(executor.store.dropped_jobs) == 0
    assert (MOCK_JOB_KEY, task_key) in executor.store.added_jobs
    assert executor.store.get_or_create() == expected_stored_dict


def test_sync_drops_finished_from_tracking(executor, task_key):
    '''
    On sync() call, any job with a completed status should
    be dropped from tracking
    '''

    from drmaa import JobState  # type: ignore

    MOCK_JOB_KEY = 12345

    mock_job_store = MockJobStore()
    mock_job_store.add_job(MOCK_JOB_KEY, task_key)

    executor.store = mock_job_store
    executor.session.jobStatus = MagicMock(return_value=JobState.DONE)

    executor.sync()

    assert MOCK_JOB_KEY in executor.store.dropped_jobs
    assert (MOCK_JOB_KEY, task_key) in executor.store.added_jobs
    assert executor.active_jobs == 0
    assert executor.store.get_or_create() == {}


def test_exception_raised_when_invalid_job_status(executor, task_key):
    '''
    If unknown state returned from DRMAA, then we don't
    expect any updates to be applied to the job

    Jobs should remain active
    '''

    MOCK_JOB_KEY = 12345
    INVALID_JOB_STATE = 999999

    executor.store = MockJobStore()
    executor.store.add_job(MOCK_JOB_KEY, task_key)

    executor.session.jobStatus = MagicMock(return_value=INVALID_JOB_STATE)

    executor.sync()

    expected_jobs = {MOCK_JOB_KEY: task_key}

    assert executor.active_jobs == 1
    assert executor.store.get_or_create() == expected_jobs
    assert len(executor.store.dropped_jobs) == 0

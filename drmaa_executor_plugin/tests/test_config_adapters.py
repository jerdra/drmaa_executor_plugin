"""
Tests for config_adapters.py to ensure that mapping
from DRM-specific configuration to DRMAA spec works
correctly
"""

import pytest
from drmaa_executor_plugin.drmaa_patches import (PatchedJobTemplate as
                                                 JobTemplate)
from drmaa_executor_plugin.config_adapters import SlurmConfig


@pytest.fixture()
def job_template():
    jt = JobTemplate()
    yield jt
    jt.delete()


def test_slurm_config_transforms_to_drmaa(job_template):
    '''
    Check whether SLURM adapter class correctly
    transforms SLURM specs to DRMAA attributes
    '''

    error = "TEST_VALUE"
    output = "TEST_VALUE"
    time = "10:00:00"
    job_name = "FAKE_JOB"

    expected_drmaa_attrs = {
        "errorPath": error,
        "outputPath": output,
        "hardWallclockTimeLimit": "10:00:00",
        "jobName": job_name
    }

    slurm_config = SlurmConfig(error=error,
                               output=output,
                               time=time,
                               job_name=job_name)

    jt = slurm_config.get_drmaa_config(job_template)

    # Test attributes matches what is expected
    for k, v in expected_drmaa_attrs.items():
        assert getattr(jt, k) == v


def test_slurm_config_native_spec_transforms_correctly(job_template):
    '''
    Test whether scheduler-specific configuration is transformed
    into nativeSpecification correctly
    '''

    job_name = "TEST"
    time = "01:00"
    account = "TEST"
    cpus_per_task = 5
    slurm_config = SlurmConfig(job_name=job_name,
                               time=time,
                               account=account,
                               cpus_per_task=cpus_per_task)

    jt = slurm_config.get_drmaa_config(job_template)
    for spec in ['account=TEST', 'cpus-per-task=5']:
        assert spec in jt.nativeSpecification


def test_invalid_timestr_fails():
    job_name = "TEST"
    time = "FAILURE"
    account = "TEST"
    cpus_per_task = 10

    with pytest.raises(ValueError):
        SlurmConfig(job_name=job_name,
                    time=time,
                    account=account,
                    cpus_per_task=cpus_per_task)


def test_timestr_not_string_fails():
    job_name = "TEST"
    time = 10
    account = "TEST"
    cpus_per_task = 10

    with pytest.raises(TypeError):
        SlurmConfig(job_name=job_name,
                    time=time,
                    account=account,
                    cpus_per_task=cpus_per_task)

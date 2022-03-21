"""
Configuration adapters for mapping native specifications from DRM to DRMAA API
"""

from __future__ import annotations
from typing import (List, ClassVar, Union, Optional, TYPE_CHECKING)

from dataclasses import dataclass, asdict, fields, InitVar
from abc import ABC, abstractmethod
import re

if TYPE_CHECKING:
    from drmaa import JobTemplate

# DRMAA specific fields, anything else should be put into native spec
DRMAA_FIELDS = [
    "email", "deadlineTime", "errorPath", "hardRunDurationLimit",
    "hardWallclockTimeLimit", "inputPath", "outputPath", "jobCategory",
    "jobName", "outputPath", "workingDirectory", "transferFiles",
    "remoteCommand", "args", "jobName", "jobCategory", "blockEmail"
]

TIMESTR_VALIDATE = re.compile("^(\\d+:)?[0-9][0-9]:[0-9][0-9]$")


@dataclass
class DRMAACompatible(ABC):
    '''
    Abstract dataclass for mapping DRM specific configuration to a
    DRMAA compatible specification

    Properties:
        _mapped_fields: List of DRM specific keys to re-map onto
            the DRMAA specification if used. Preferably users will
            use the DRMAA variant of these specifications rather than
            the corresponding native specification
    '''

    _mapped_fields: ClassVar[List[str]]

    def __str__(self):
        '''
        Display formatted configuration for executor
        '''
        attrs = asdict(self)
        drmaa_fields = "\n".join([
            f"{field}:\t{attrs.get(field)}" for field in DRMAA_FIELDS
            if attrs.get(field) is not None
        ])

        drm_fields = "\n".join([
            f"{field}:\t{attrs.get(field)}" for field in self._native_fields()
            if attrs.get(field) is not None
        ])

        return ("DRMAA Config:\n" + drmaa_fields + "\nNative Specification\n" +
                drm_fields)

    def get_drmaa_config(self, jt: JobTemplate) -> JobTemplate:
        '''
        Apply settings onto DRMAA JobTemplate
        '''

        for field in DRMAA_FIELDS:
            value = getattr(self, field, None)
            if value is not None:
                setattr(jt, field, value)

        jt.nativeSpecification = self.drm2drmaa()
        return jt

    @abstractmethod
    def drm2drmaa(self) -> str:
        '''
        Build native specification from DRM-specific fields
        '''

    def _native_fields(self):
        return [
            f for f in asdict(self).keys()
            if (f not in self._mapped_fields) and (f not in DRMAA_FIELDS)
        ]

    def set_fields(self, **drmaa_kwargs):
        for field, value in drmaa_kwargs.items():
            if field not in DRMAA_FIELDS:
                raise AttributeError(
                    "Malformed adapter class! Cannot map field"
                    f" {field} to a DRMAA-compliant field")

            setattr(self, field, value)


@dataclass
class DRMAAConfig(DRMAACompatible):
    def drm2drmaa(self):
        return


@dataclass
class SlurmConfig(DRMAACompatible):
    '''
    Transform SLURM resource specification into DRMAA-compliant inputs

    References:
        See https://github.com/natefoo/slurm-drmaa for native specification
        details
    '''

    _mapped_fields: ClassVar[List[str]] = {
        "error", "output", "job_name", "time"
    }

    job_name: InitVar[str]
    time: InitVar[str]
    error: InitVar[str] = None
    output: InitVar[str] = None

    account: Optional[str] = None
    acctg_freq: Optional[str] = None
    comment: Optional[str] = None
    constraint: Optional[List] = None
    cpus_per_task: Optional[int] = None
    contiguous: Optional[bool] = None
    dependency: Optional[List] = None
    exclusive: Optional[bool] = None
    gres: Optional[Union[List[str], str]] = None
    no_kill: Optional[bool] = None
    licenses: Optional[List[str]] = None
    clusters: Optional[Union[List[str], str]] = None
    mail_type: Optional[str] = None
    mem: Optional[int] = None
    mincpus: Optional[int] = None
    nodes: Optional[int] = None
    ntasks: Optional[int] = None
    no_requeue: Optional[bool] = None
    ntasks_per_node: Optional[int] = None
    partition: Optional[int] = None
    qos: Optional[str] = None
    requeue: Optional[bool] = None
    reservation: Optional[str] = None
    share: Optional[bool] = None
    tmp: Optional[str] = None
    nodelist: Optional[Union[List[str], str]] = None
    exclude: Optional[Union[List[str], str]] = None

    def __post_init__(self, job_name, time, error, output):
        '''
        Transform Union[List[str]] --> comma-delimited str
        '''

        _validate_timestr(time, "time")
        super().set_fields(jobName=job_name,
                           hardWallclockTimeLimit=time,
                           errorPath=error,
                           outputPath=output)

        self.job_name = job_name
        self.time = time
        self.error = error
        self.output = output

        for field in fields(self):
            value = getattr(self, field.name)
            if field.type == Union[List[str], str] and isinstance(value, list):
                setattr(self, field.name, ",".join(value))

    def drm2drmaa(self) -> str:
        return self._transform_attrs()

    def _transform_attrs(self) -> str:
        '''
        Remap named attributes to "-" form, excludes renaming
        DRMAA-compliant fields (set in __post_init__()) then join
        attributes into a nativeSpecification string
        '''

        out = []
        for field in self._native_fields():

            value = getattr(self, field)
            if value is None:
                continue

            field_fmtd = field.replace("_", "-")
            if isinstance(value, bool):
                out.append(f"--{field_fmtd}")
            else:
                out.append(f"--{field_fmtd}={value}")
        return " ".join(out)


def _timestr_to_sec(timestr: str) -> int:
    '''
    Transform a time-string (D-HH:MM:SS) --> seconds
    '''

    days = 0
    if "-" in timestr:
        day_str, timestr = timestr.split('-')
        days = int(day_str)

    seconds = (24 * days) * (60**2)
    for exp, unit in enumerate(reversed(timestr.split(":"))):
        seconds += int(unit) * (60**exp)

    return seconds


def _validate_timestr(timestr: str, field_name: str) -> str:
    '''
    Validate timestring to make sure it meets
    expected format.
    '''

    if not isinstance(timestr, str):
        raise TypeError(f"Expected {field_name} to be of type string "
                        f"but received {type(timestr)}!")

    result = TIMESTR_VALIDATE.match(timestr)
    if not result:
        raise ValueError(f"Expected {field_name} to be of format "
                         "X...XX:XX:XX or XX:XX! "
                         f"but received {timestr}")

    return timestr

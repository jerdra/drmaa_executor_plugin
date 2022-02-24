'''
Patches on DRMAA-python module
'''

from drmaa import JobTemplate, Session
from drmaa.helpers import Attribute, IntConverter


class PatchedIntConverter():
    '''
    Helper class to correctly encode Integer values
    as little-endian bytes for Python 3

    Info:
        The standard IntConverter class attempts to convert
        integer values to bytes using `bytes(value)` which
        results in a zero'd byte-array of length `value`.
    '''
    @staticmethod
    def to_drmaa(value: int) -> bytes:
        return value.to_bytes(8, byteorder="little")

    @staticmethod
    def from_drmaa(value: bytes) -> int:
        return int.from_bytes(value, byteorder="little")


class PatchedJobTemplate(JobTemplate):
    def __init__(self):
        '''
        Dynamically patch attributes using IntConverter
        '''
        super(PatchedJobTemplate, self).__init__()
        for attr, value in vars(JobTemplate).items():
            if isinstance(value, Attribute):
                if value.converter is IntConverter:
                    setattr(value, "converter", PatchedIntConverter)


class PatchedSession(Session):
    '''
    Override createJobTemplate method to return
    Patched version
    '''
    @staticmethod
    def createJobTemplate() -> PatchedJobTemplate:
        return PatchedJobTemplate()

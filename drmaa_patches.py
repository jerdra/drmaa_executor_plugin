'''
Patches on DRMAA-python module
'''

from drmaa import JobTemplate, Session
from drmaa.helpers import Attribute, IntConverter


class PatchedIntConverter():
    '''
    Helper class to correctly encode Integer values
    as little-endian bytes for Python 3
    '''
    @staticmethod
    def to_drmaa(value: int) -> bytes:
        return value.to_bytes(8, byteorder="little")

    @staticmethod
    def from_drmaa(value: int) -> bytes:
        return int.from_bytes(value, byteorder="little")


class PatchedJobTemplate(JobTemplate):
    def __init__(self):
        '''
        Dynamically patch IntConverter attributes
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
    def createJobTemplate(self) -> PatchedJobTemplate:
        return PatchedJobTemplate()

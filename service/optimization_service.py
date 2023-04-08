import os

from service.base_provider import BaseProvider


class OptimizationService(BaseProvider):
    def __init__(self):
        super().__init__(objects={}, directory=os.path.dirname(__file__), prefix='dump')

    
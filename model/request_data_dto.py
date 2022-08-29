from pydantic import BaseModel


class RequestDataDto(BaseModel):
    dataRepositoryURL: str
    computationURL: str
    date_range: str
    geometry: dict
    scene_cloud_tolerance: float = 50
    nodes: int = 40

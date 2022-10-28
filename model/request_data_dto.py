from pydantic import BaseModel


class RequestDataDto(BaseModel):
    dataRepositoryURL: str
    computationURL: str
    date_range: str
    geometry: dict
    epsg: int = 4326
    chunk_size: tuple = (1024, 1024)
    scene_cloud_tolerance: float = 50
    nodes: int = 40

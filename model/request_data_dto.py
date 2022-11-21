from pydantic import BaseModel


class RequestDataDto(BaseModel):
    dataRepositoryURL: str
    computationURL: str
    date_range: str
    coords: dict
    epsg: int = 4326
    chunk_size: tuple = (2048, 2048)
    scene_cloud_tolerance: float = 50
    nodes: int = 16

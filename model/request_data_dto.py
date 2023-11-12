from pydantic import BaseModel


class RequestDataDto(BaseModel):
    dataRepositoryURL: str
    computationURL: str
    date_range: str
    coords: dict
    epsg: int = 4326
    chunk_size: tuple = 'auto'
    scene_cloud_tolerance: float = 50
    nodes: int = 16
    cores: int = 4
    ram: int = 8
    cmap: str = 'RdYlGn'
    fire_and_forget: bool = False

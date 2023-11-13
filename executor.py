from satsearch import Search

from functions.evi import EVI
from functions.ndvi import NDVI
from functions.ndwi import NDWI


class Executor:

    def __init__(self, stac_api_url, function, bbox, epsg: int, chunk_size: tuple):
        self.stac_api_url = stac_api_url
        self.function = function
        self.bbox = bbox
        self.epsg = epsg
        self.chunk_size = chunk_size

    def search(self, date_range, scene_cloud_tolerance):
        search = Search(bbox=self.bbox, datetime=date_range,
                        query={"eo:cloud_cover": {"lt": scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=self.stac_api_url)
        items = search.items()
        return items

    def graph(self, item):
        meta = self.get_function_meta(item)
        return meta.generate_graph()

    def average_graph(self, item):
        meta = self.get_function_meta(item)
        return meta.generate_average_graph()

    def get_function_meta(self, item):
        if self.function == 'ndvi':
            return NDVI(item, self.epsg, self.chunk_size, self.bbox)
        if self.function == 'ndwi':
            return NDWI(item, self.epsg, self.chunk_size, self.bbox)
        if self.function == 'evi':
            return EVI(item, self.epsg, self.chunk_size, self.bbox)
        raise Exception(f'method {self.function} not implemented!')

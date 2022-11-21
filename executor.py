from satsearch import Search

from functions.ndvi import NDVI


class Executor:

    def __init__(self, stac_api_url, function, epsg: int, chunk_size: tuple):
        self.stac_api_url = stac_api_url
        self.function = function
        self.epsg = epsg
        self.chunk_size = chunk_size

    def search(self, bbox, date_range, scene_cloud_tolerance):
        search = Search(bbox=bbox, datetime=date_range,
                        query={"eo:cloud_cover": {"lt": scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=self.stac_api_url)
        items = search.items()
        return items

    def execute(self, item):
        meta = self.get_function_meta(item)
        return meta.generate_graph().compute()

    def get_function_meta(self, item):
        if self.function == 'ndvi':
            return NDVI(item, self.epsg, self.chunk_size)
        raise Exception(f'method {self.function} not implemented!')

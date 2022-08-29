from satsearch import Search

from functions.ndvi import NDVI


class Executor:

    def __init__(self, stac_api_url):
        self.stac_api_url = stac_api_url

    def search(self, bbox, date_range, scene_cloud_tolerance):
        search = Search(bbox=bbox, datetime=date_range,
                        query={"eo:cloud_cover": {"lt": scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=self.stac_api_url)
        items = search.items()
        return items

    def execute(self, item):
        ndvi = NDVI(item, 4326, (1024, 1024))
        return ndvi.extract_bands().compute()

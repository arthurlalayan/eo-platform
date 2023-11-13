import stackstac


class NDVI:

    def __init__(self, item, epsg, chunk_size, bbox):
        self.item = item
        self.epsg = epsg
        self.chunk_size = chunk_size
        self.bbox = bbox

    def generate_graph(self):
        stack = stackstac.stack(self.item, epsg=self.epsg, chunksize=self.chunk_size)
        nir, red = stack.sel(band="B08").squeeze(), stack.sel(band="B04").squeeze()
        return ((nir - red) / (nir + red)).loc[::-1, :]

    def generate_average_graph(self):
        data = stackstac.stack(self.item,
                               epsg=self.epsg,
                               chunksize=self.chunk_size,
                               bounds_latlon=self.bbox,
                               assets=["B08", "B04"])
        stack = data.mean('time')
        nir, red = stack.sel(band="B08"), stack.sel(band="B04")
        return ((nir - red) / (nir + red)).loc[::-1, :]

import stackstac


class NDWI:

    def __init__(self, item, epsg, chunk_size, bbox):
        self.item = item
        self.epsg = epsg
        self.chunk_size = chunk_size
        self.bbox = bbox

    def generate_graph(self):
        stack = stackstac.stack(self.item, epsg=self.epsg, chunksize=self.chunk_size)
        nir, green = stack.sel(band="B08"), stack.sel(band="B03")
        return (green - nir) / (green + nir)

    def generate_average_graph(self):
        data = stackstac.stack(self.item, epsg=self.epsg, chunksize=self.chunk_size, bounds_latlon=self.bbox,
                               assets=["B08", "B03"])
        stack = data.mean('time')
        nir, green = stack.sel(band="B08"), stack.sel(band="B03")
        return ((green - nir) / (green + nir)).loc[::-1, :]

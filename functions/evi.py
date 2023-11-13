import stackstac


class EVI:

    def __init__(self, item, epsg, chunk_size, bbox):
        self.item = item
        self.epsg = epsg
        self.chunk_size = chunk_size
        self.bbox = bbox

    def generate_graph(self):
        stack = stackstac.stack(self.item, epsg=self.epsg, chunksize=self.chunk_size)
        nir, red, blue = stack.sel(band="B08"), stack.sel(band="B04"), stack.sel(band="B02")
        return (2.5 * (nir - red) / (nir + 6 * red - 7.5 * blue + 1)).loc[::-1, :]

    def generate_average_graph(self):
        data = stackstac.stack(self.item, epsg=self.epsg, chunksize=self.chunk_size, bounds_latlon=self.bbox,
                               assets=["B08", "B04", "B02"])
        stack = data.mean('time')
        nir, red, blue = stack.sel(band="B08"), stack.sel(band="B04"), stack.sel(band="B02")
        return (2.5 * (nir - red) / (nir + 6 * red - 7.5 * blue + 1)).loc[::-1, :]

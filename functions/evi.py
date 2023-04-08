import stackstac


class EVI:

    def __init__(self, item, epsg, chunk_size):
        self.item = item
        self.epsg = epsg
        self.chunk_size = chunk_size

    def generate_graph(self):
        stack = stackstac.stack(self.item, epsg=self.epsg, chunksize=self.chunk_size)
        nir, red, blue = stack.sel(band="B08"), stack.sel(band="B04"), stack.sel(band="B02")
        return 2.5 * (nir - red) / (nir + 6 * red - 7.5 * blue + 1)

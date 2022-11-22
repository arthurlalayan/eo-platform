import stackstac


# from functions.base_function import BaseFunction


class NDVI:

    def __init__(self, item, epsg, chunk_size):
        self.item = item
        self.epsg = epsg
        self.chunk_size = chunk_size

    def generate_graph(self):
        stack = stackstac.stack(self.item, epsg=self.epsg, chunksize=self.chunk_size)
        nir, red = stack.sel(band="B08"), stack.sel(band="B04")
        return (nir - red) / (nir + red)

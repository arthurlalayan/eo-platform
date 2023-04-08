import stackstac



class NDWI:

    def __init__(self, item, epsg, chunk_size):
        self.item = item
        self.epsg = epsg
        self.chunk_size = chunk_size

    def generate_graph(self):
        stack = stackstac.stack(self.item, epsg=self.epsg, chunksize=self.chunk_size)
        nir, green = stack.sel(band="B08"), stack.sel(band="B03")
        return (green - nir) / (green + nir)

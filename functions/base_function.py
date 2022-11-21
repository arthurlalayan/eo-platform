class BaseFunction:
    def __init__(self, item, epsg, chunk_size):
        self.item = item
        self.epsg = epsg
        self.chunk_size = chunk_size

    def generate_graph(self):
        pass

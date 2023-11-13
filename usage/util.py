import folium
import shapely.geometry
from matplotlib import pyplot as plt
import json
import numpy as np

def convert_bounds(bbox, invert_y=False):
    x1, y1, x2, y2 = bbox
    if invert_y:
        y1, y2 = y2, y1
    return ((y1, x1), (y2, x2))

def visualize(meta_file_name, show_query=True):
  map = folium.Map()
  with open(meta_file_name, encoding = 'utf-8') as f:
    data = json.loads(f.read())
  for i, im in enumerate(data):
    bbox = im.get('bbox')
    if show_query:
        folium.GeoJson(
          shapely.geometry.box(*bbox),
          style_function=lambda x: dict(fill=True, weight=2, opacity=0.8, color="blue"),
          name=f"Query_{i}",
        ).add_to(map)

    image_bounds = convert_bounds(bbox, invert_y=True)
    img = plt.imread(im.get('filename'), format='jpg')
    img_ovr = folium.raster_layers.ImageOverlay(
            name=f"NDVI_{i}",
            image=img,
            bounds=image_bounds,
            opacity=0.9,
            interactive=True,
            cross_origin=False,
            zindex=1,
    )
    img_ovr.add_to(map)
    map.fit_bounds(bounds=image_bounds)
      
  folium.LayerControl().add_to(map)
  return map

def show_legend(cmap_name):
    fig, ax = plt.subplots(figsize=(20, 0.5))
    cmap=plt.cm.get_cmap(cmap_name)
    values = np.linspace(-1, 1, 256).reshape(1, -1)
    img = ax.imshow(values, cmap=cmap, aspect='auto')
    ax.set_axis_off()
    cbar = plt.colorbar(img, orientation='horizontal', pad=0.05, aspect=300)
    plt.show();
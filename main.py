import json
import time

import stackstac
from distributed import wait
from fastapi import FastAPI
from satsearch import Search

from executor import Executor
from model.request_data_dto import RequestDataDto
from service.computation_provider import create_cluster
from utils.util import zip_files, create_figure, get_bbox

app = FastAPI()


@app.post("/ndvifull")
async def ndvi_full(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.coords)
        # executor = Executor(data.dataRepositoryURL)
        search = Search(bbox=bbox, datetime=data.date_range,
                        query={"eo:cloud_cover": {"lt": data.scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=data.dataRepositoryURL)
        items = search.items()

        stack = stackstac.stack(items, epsg=data.epsg, chunksize=data.chunk_size, bounds=bbox, assets=["B08", "B04"])
        nir, red = stack.sel(band="B08"), stack.sel(band="B04")
        start_time = time.time()
        try:
            ndvi = ((nir - red) / (nir + red))
            ndvi = wait(ndvi.persist())
        except Exception as e:
            print(e)
        end_time = time.time()
        return {'exec_time': end_time - start_time}
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.post("/{function}")
async def compute(data: RequestDataDto, function: str):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.coords)
        executor = Executor(data.dataRepositoryURL, function, data.epsg, data.chunk_size)
        items = executor.search(bbox, data.date_range, data.scene_cloud_tolerance)
        results = []
        meta = []
        for item in items:
            start_time = time.time()
            result = executor.execute(item)
            end_time = time.time()
            results.append({function: result, 'exec_time': end_time - start_time})
        filenames = []
        for i, result in enumerate(results):
            filename = f"{function}_{i}.png"
            filenames.append(filename)
            create_figure(result.get(function).to_numpy(), filename)
            meta.append({'exec_time': result.get('exec_time'), 'bbox': items[i].bbox, 'filename': filename,
                         'date': items[i].date.strftime("%m/%d/%Y")})
        with open('meta.json', 'w', encoding='utf8') as json_file:
            json.dump(meta, json_file, ensure_ascii=False)
            filenames.append('meta.json')
        return zip_files(filenames)
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.get("/test")
def test():
    return {"Status": "Up"}

import json
import os
import threading
import time

import matplotlib.pyplot as plt
import stackstac
from colorcet import palette
from dask.distributed import wait
from dask_gateway import GatewayCluster
from datashader.transfer_functions import shade
from datashader.utils import export_image
from fastapi import FastAPI
from satsearch import Search

from executor import Executor
from model.request_data_dto import RequestDataDto
from service.computation_provider import create_cluster
from utils.util import zip_files, create_figure, get_bbox

app = FastAPI()

output_dir = 'output'


@app.post("/avg/{function}")
async def compute(data: RequestDataDto, function: str):
    cluster = create_cluster(data)

    try:
        bbox = get_bbox(data.coords)
        executor = Executor(data.dataRepositoryURL, function, bbox, data.epsg, data.chunk_size)
        items = executor.search(data.date_range, data.scene_cloud_tolerance)

        start_time = time.time()

        task_id = str(int(start_time))
        image_name = f"{function}_{task_id}"
        output_path = os.path.join(output_dir, task_id)
        os.makedirs(output_path, exist_ok=True)
        meta_file_name = f"meta_{function}_{int(start_time)}.json"

        if data.fire_and_forget:
            (threading.Thread(target=create_output,
                              args=(executor, items, start_time, data, image_name, cluster, bbox, output_path,
                                    meta_file_name))
             .start())
            return {
                "dask_dashboard_url": cluster.dashboard_link,
                "task_id": task_id
            }

        create_output(executor, items, start_time, data, image_name, cluster, bbox, output_path, meta_file_name)

        return zip_files(task_id)
    except Exception as e:
        print(e)
        cluster.shutdown()


def create_output(executor, items, start_time, data, image_name, cluster, bbox, output_path, meta_file_name):
    meta_file_path = os.path.join(output_path, meta_file_name)
    try:
        result = executor.average_graph(items).persist()
        wait(result)
        end_time = time.time()
        image = shade(result, cmap=plt.cm.get_cmap(data.cmap))
        export_image(img=image, filename=image_name, fmt=".png", export_path=os.path.join(".", output_path))
        meta = []
        file_name = f"{image_name}.png"
        meta.append({'exec_time': end_time - start_time, 'bbox': bbox, 'filename': file_name})
        with open(meta_file_path, 'w', encoding='utf8') as json_file:
            json.dump(meta, json_file, ensure_ascii=False)
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.get("/output/{task_id}")
def get_result(task_id: str):
    return zip_files(task_id)


@app.post("/{function}")
async def compute(data: RequestDataDto, function: str):
    cluster = create_cluster(data)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    calculate(data, function, cluster)


@app.post("/ndvi/shader")
async def compute(data: RequestDataDto):
    cluster = create_cluster(data)
    try:
        bbox = get_bbox(data.coords)
        search = Search(bbox=bbox, datetime=data.date_range,
                        query={"eo:cloud_cover": {"lt": data.scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=data.dataRepositoryURL)
        items = search.items()
        filenames = []
        meta = []
        request_time = time.time()
        for i, item in enumerate(items):
            start_time = time.time()
            filename = f"ndvi_{request_time}_{i}"
            stack = stackstac.stack(item, epsg=data.epsg, chunksize=data.chunk_size)
            nir, red = stack.sel(band="B08").squeeze(), stack.sel(band="B04").squeeze()
            result = (nir - red) / (nir + red)
            result.persist()
            cmap = palette[data.cmap]
            img = shade(result, cmap)
            export_image(img=img, filename=filename, fmt=".png", export_path=".")
            end_time = time.time()
            image_name = f"{filename}.png"
            filenames.append(image_name)
            meta.append({'exec_time': end_time - start_time, 'bbox': items[i].bbox, 'filename': image_name,
                         'date': items[i].date.strftime("%m/%d/%Y")})
        with open('meta.json', 'w', encoding='utf8') as json_file:
            json.dump(meta, json_file, ensure_ascii=False)
            filenames.append('meta.json')
        return zip_files(filenames)
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


def calculate(data: RequestDataDto, function: str, cluster: GatewayCluster):
    try:
        bbox = get_bbox(data.coords)
        executor = Executor(data.dataRepositoryURL, function, bbox, data.epsg, data.chunk_size)
        items = executor.search(data.date_range, data.scene_cloud_tolerance)
        results = []
        meta = []
        for item in items:
            start_time = time.time()
            result = executor.execute(item)
            end_time = time.time()
            results.append({function: result, 'exec_time': end_time - start_time, 'bbox': item.bbox})
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

import json
import os
import threading
import time

import matplotlib.pyplot as plt
from dask.distributed import wait
from datashader.transfer_functions import shade
from datashader.utils import export_image
from fastapi import FastAPI

from executor import Executor
from model.request_data_dto import RequestDataDto
from service.computation_provider import create_cluster
from utils.util import zip_files, get_bbox

app = FastAPI()

output_dir = 'output'


@app.post("/avg/{function}")
async def compute_average(data: RequestDataDto, function: str):
    bbox = get_bbox(data.coords)
    executor = Executor(data.dataRepositoryURL, function, bbox, data.epsg, data.chunk_size)
    items = executor.search(data.date_range, data.scene_cloud_tolerance)

    if not len(items):
        return "No items found!"

    cluster = create_cluster(data)
    try:
        start_time = time.time()

        task_id = str(int(start_time))
        image_name = f"{function}_{task_id}"
        output_path = os.path.join(output_dir, task_id)
        os.makedirs(output_path, exist_ok=True)
        meta_file_name = f"meta_{function}_{int(start_time)}.json"

        if data.fire_and_forget:
            (threading.Thread(target=create_average_output,
                              args=(executor, items, start_time, data, image_name,
                                    cluster, bbox, output_path, meta_file_name))
             .start())
            return {
                "dask_dashboard_url": cluster.dashboard_link,
                "task_id": task_id
            }

        create_average_output(executor, items, start_time, data, image_name, cluster, bbox, output_path, meta_file_name)

        return zip_files(task_id)
    except Exception as e:
        print(e)
        cluster.shutdown()


def create_average_output(executor, items, start_time, data, image_name, cluster, bbox, output_path, meta_file_name):
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
    bbox = get_bbox(data.coords)
    executor = Executor(data.dataRepositoryURL, function, bbox, data.epsg, data.chunk_size)
    items = executor.search(data.date_range, data.scene_cloud_tolerance)

    if not len(items):
        return "No items found!"

    cluster = create_cluster(data)
    try:
        request_time = time.time()
        task_id = str(int(request_time))
        output_path = os.path.join(output_dir, task_id)
        os.makedirs(output_path, exist_ok=True)
        meta_file_name = f"meta_{function}_{int(request_time)}.json"
        meta_file_path = os.path.join(output_path, meta_file_name)

        if data.fire_and_forget:
            (threading.Thread(target=create_tile_output,
                              args=(cluster, items, executor, function, task_id, data, output_path, meta_file_path))
             .start())
            return {
                "dask_dashboard_url": cluster.dashboard_link,
                "task_id": task_id
            }

        create_tile_output(cluster, items, executor, function, task_id, data, output_path, meta_file_path)

        return zip_files(task_id)
    except Exception as e:
        print(e)
        cluster.shutdown()


def create_tile_output(cluster, items, executor, function, task_id, data, output_path, meta_file_path):
    try:
        meta = []
        for i, item in enumerate(items):
            start_time = time.time()
            result = executor.graph(item).persist()
            wait(result)
            end_time = time.time()
            image_name = f"{function}_{task_id}_{i}"
            image = shade(result, cmap=plt.cm.get_cmap(data.cmap))
            export_image(img=image, filename=image_name, fmt=".png", export_path=os.path.join(".", output_path))
            file_name = f"{image_name}.png"
            meta.append({'exec_time': end_time - start_time, 'bbox': items[i].bbox, 'filename': file_name, 'index': i})
        with open(meta_file_path, 'w', encoding='utf8') as json_file:
            json.dump(meta, json_file, ensure_ascii=False)
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.get("/health")
def health():
    return {"Status": "Up"}

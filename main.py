import json
import time

import numpy as np
import stackstac
from distributed import Client
from fastapi import FastAPI
from satsearch import Search

from executor import Executor
from model.request_data_dto import RequestDataDto
from service.computation_provider import create_cluster
from utils.util import zip_files, create_figure, get_bbox

app = FastAPI()


def base(data: RequestDataDto, func):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        executor = Executor(data.dataRepositoryURL)
        items = executor.search(bbox, data.date_range, data.scene_cloud_tolerance)
        results = []
        for i, item in enumerate(items):
            start_time = time.time()
            stack = stackstac.stack(item, epsg=4326, chunksize=data.chunk_size)
            nir, red = stack.sel(band="B08"), stack.sel(band="B04")
            res = func(nir, red)
            end_time = time.time()
            results.append({'exec_time': end_time - start_time, 'bbox': item.bbox,
                            'date': item.date.strftime("%m/%d/%Y"), 'index': i})
        return results
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.post("/mix")
async def mix(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        executor = Executor(data.dataRepositoryURL)
        items = executor.search(bbox, data.date_range, data.scene_cloud_tolerance)
        results = []
        for i, item in enumerate(items):
            start_time = time.time()
            stack = stackstac.stack(item, epsg=4326, chunksize=data.chunk_size)
            nir, red = stack.sel(band="B08"), stack.sel(band="B04")
            sum = (nir + red).compute()
            results.append({'exec_time': time.time() - start_time, 'index': i, 'op': 'sum'})
            start_time = time.time()
            sub = (nir - red).compute()
            results.append({'exec_time': time.time() - start_time, 'index': i, 'op': 'sub'})
            start_time = time.time()
            ndvi = (sum / sub)
            results.append({'exec_time': time.time() - start_time, 'index': i, 'op': 'sub'})

        return results
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.post("/sum")
async def sum(data: RequestDataDto):
    return base(data, lambda nir, red: (nir + red).compute())


@app.post("/sub")
async def sub(data: RequestDataDto):
    return base(data, lambda nir, red: (nir - red).compute())


@app.post("/mul")
async def mul(data: RequestDataDto):
    return base(data, lambda nir, red: (nir * red).compute())


@app.post("/div")
async def div(data: RequestDataDto):
    return base(data, lambda nir, red: (nir / red).compute())


@app.post("/mem")
async def mem(data: RequestDataDto):
    return base(data, load_into_memory)


@app.post("/ndvifull")
async def mem(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        executor = Executor(data.dataRepositoryURL)
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
            ndvi = ndvi.compute()
        except Exception as e:
            print(e)
        end_time = time.time()
        return {'exec_time': end_time - start_time}
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.post("/ndvi_season")
async def ndvi_season(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = Client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        executor = Executor(data.dataRepositoryURL)
        search = Search(bbox=bbox, datetime=data.date_range,
                        query={"eo:cloud_cover": {"lt": data.scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=data.dataRepositoryURL)
        items = search.items()

        stack = stackstac.stack(items, epsg=data.epsg, chunksize=data.chunk_size, bounds=bbox, assets=["B08", "B04"])
        size = (np.prod(stack.shape) / 1024 / 1024 * 8)
        nir, red = stack.sel(band="B08"), stack.sel(band="B04")
        start_time = time.time()
        try:
            ndvi = ((nir - red) / (nir + red))
            ndvi = ndvi.mean("time").compute()
            print(ndvi)
            # ndvi = ndvi.mean()
            # print(ndvi)
        except Exception as e:
            print(e)
        end_time = time.time()
        return {'exec_time': end_time - start_time, 'size': size, 'shape': stack.shape}
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()

@app.post("/ndvi_season_mean")
async def ndvi_season_mean(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = Client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        executor = Executor(data.dataRepositoryURL)
        search = Search(bbox=bbox, datetime=data.date_range,
                        query={"eo:cloud_cover": {"lt": data.scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=data.dataRepositoryURL)
        items = search.items()

        stack = stackstac.stack(items, epsg=data.epsg, chunksize=data.chunk_size, bounds=bbox, assets=["B08", "B04"])
        size = (np.prod(stack.shape) / 1024 / 1024 * 8)
        nir, red = stack.sel(band="B08"), stack.sel(band="B04")
        start_time = time.time()
        try:
            ndvi = ((nir - red) / (nir + red))
            ndvi = ndvi.mean("time").mean().compute()
            print(ndvi)
            # ndvi = ndvi.mean()
            # print(ndvi)
        except Exception as e:
            print(e)
        end_time = time.time()
        return {'exec_time': end_time - start_time, 'size': size, 'shape': stack.shape}
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()

@app.post("/ndvi_season_just_mean")
async def ndvi_season_just_mean(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = Client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        executor = Executor(data.dataRepositoryURL)
        search = Search(bbox=bbox, datetime=data.date_range,
                        query={"eo:cloud_cover": {"lt": data.scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=data.dataRepositoryURL)
        items = search.items()

        stack = stackstac.stack(items, epsg=data.epsg, chunksize=data.chunk_size, bounds=bbox, assets=["B08", "B04"])
        size = (np.prod(stack.shape) / 1024 / 1024 * 8)
        nir, red = stack.sel(band="B08"), stack.sel(band="B04")
        start_time = time.time()
        try:
            ndvi = ((nir - red) / (nir + red))
            ndvi = ndvi.mean().compute()
            print(ndvi)
            # ndvi = ndvi.mean()
            # print(ndvi)
        except Exception as e:
            print(e)
        end_time = time.time()
        return {'exec_time': end_time - start_time, 'size': size, 'shape': stack.shape}
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


def load_into_memory(nir, red):
    nir = nir.persist()
    red = red.persist()
    return None


@app.post("/ndvi")
async def ndvi(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        executor = Executor(data.dataRepositoryURL)
        items = executor.search(bbox, data.date_range, data.scene_cloud_tolerance)
        # delayed_results = []
        results = []
        meta = []
        for item in items:
            start_time = time.time()
            ndvi_result = executor.execute(item)
            end_time = time.time()
            results.append({'ndvi': ndvi_result, 'exec_time': end_time - start_time})

        #     delayed_results.append(delayed(calculate)(executor, item))
        filenames = []
        # results = dask.compute(*delayed_results)
        for i, result in enumerate(results):
            filename = "ndvi_cmap_{}.png".format(i)
            filenames.append(filename)
            create_figure(result.get('ndvi').to_numpy(), filename)
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


@app.post("/ndvi_1")
async def ndvi_1(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        search = Search(bbox=bbox, datetime=data.date_range,
                        query={"eo:cloud_cover": {"lt": data.scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=data.dataRepositoryURL)
        items = search.items()
        results = []
        for i, item in enumerate(items):
            start_time = time.time()
            stack = stackstac.stack(item, epsg=data.epsg, chunksize=data.chunk_size)
            nir, red = stack.sel(band="B08"), stack.sel(band="B04")
            ndvi_result = ((nir - red) / (nir + red)).compute()
            end_time = time.time()
            results.append(
                {'index': i, 'size': (np.prod(nir.shape) / 1024 / 1024 * 8 * 2), 'exec_time': end_time - start_time})
        return results
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.post("/ndvi_submit")
async def ndvi_submit(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = Client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        search = Search(bbox=bbox, datetime=data.date_range,
                        query={"eo:cloud_cover": {"lt": data.scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=data.dataRepositoryURL)
        items = search.items()
        start_time = time.time()

        stack = stackstac.stack(items, epsg=data.epsg, chunksize=data.chunk_size, assets=["B08", "B04"])
        size = (np.prod(stack.shape) / 1024 / 1024 * 8)

        def calculate(item):
            stack = stackstac.stack(item, epsg=4326, chunksize=(1024, 1024), assets=["B08", "B04"])
            nir, red = stack.sel(band="B08"), stack.sel(band="B04")
            return (nir - red) / (nir + red)

        futures = client.map(calculate, items)
        results = client.gather(futures)
        print(results[0])

        end_time = time.time()
        return {'size': size, 'exec_time': end_time - start_time}
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.post("/testt")
async def testt(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = Client()
    print(cluster.dashboard_link)
    try:
        start_time = time.time()

        # @dask.delayed
        def calculate(item):
            return item + 1

        futures = client.map(calculate, range(100))
        results = client.gather(futures)
        print(results[0])

        # futures = client.map(calculate, L)
        # results = await client.gather(futures, asynchronous=True)
        # print(results)

        # r = dask.compute(L)
        # futures = client.compute(L)

        end_time = time.time()
        return {'exec_time': end_time - start_time}

    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.post("/ndvi_same/{count}")
async def ndvi_same(data: RequestDataDto, count: int):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)
        search = Search(bbox=bbox, datetime=data.date_range,
                        query={"eo:cloud_cover": {"lt": data.scene_cloud_tolerance}},
                        collections=["sentinel-s2-l2a-cogs"],
                        url=data.dataRepositoryURL)
        items = search.items()
        results = []
        item = items[0]
        for i in range(count):
            start_time = time.time()
            stack = stackstac.stack(item, epsg=data.epsg, chunksize=data.chunk_size)
            nir, red = stack.sel(band="B08"), stack.sel(band="B04")
            ndvi_result = ((nir - red) / (nir + red)).compute()
            end_time = time.time()
            results.append(
                {'index': i, 'size': (np.prod(nir.shape) / 1024 / 1024 * 8 * 2), 'exec_time': end_time - start_time})
        return results
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


def search(stac_api_url, bbox, date_range, scene_cloud_tolerance):
    search = Search(bbox=bbox, datetime=date_range,
                    query={"eo:cloud_cover": {"lt": scene_cloud_tolerance}},
                    collections=["sentinel-s2-l2a-cogs"],
                    url=stac_api_url)
    items = search.items()
    return items


@app.post("/ndvi1")
async def ndvi1(data: RequestDataDto):
    cluster = create_cluster(data.computationURL, data.nodes)
    client = cluster.get_client()
    print(cluster.dashboard_link)
    try:
        bbox = get_bbox(data.geometry)

        # client.upload_file('executor.py')

        # @dask.delayed
        def calculate(item):
            stack = stackstac.stack(item, epsg=4326)
            nir, red = stack.sel(band="B08"), stack.sel(band="B04")
            ndvi = (nir - red) / (nir + red)
            return ndvi.compute()

        items = search(data.dataRepositoryURL, bbox, data.date_range, data.scene_cloud_tolerance)
        L = client.map(calculate, items)
        results = client.gather(L)
        meta = []
        file_names = []
        for i, result in enumerate(results):
            filename = "ndvi_cmap_{}.png".format(i)
            file_names.append(filename)
            create_figure(result.to_numpy(), filename)
            meta.append({'bbox': items[i].bbox, 'filename': filename, 'date': items[i].date.strftime("%m/%d/%Y")})
        with open('meta.json', 'w', encoding='utf8') as json_file:
            json.dump(meta, json_file, ensure_ascii=False)
            file_names.append('meta.json')
        return zip_files(file_names)
    except Exception as e:
        print(e)
    finally:
        cluster.shutdown()


@app.get("/test")
def test():
    return {"Status": "Up"}

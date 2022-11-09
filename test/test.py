import time

import numpy as np
import pandas as pd
import requests


def execute(nodes, tiles):
    kube = '128.110.223.83'
    url = f'http://{kube}:81/ndvi_same/{tiles}'
    computationURL = f'http://{kube}'
    dataRepositoryURL = 'https://earth-search.aws.element84.com/v0/'
    date_range = '2022-03-01/2022-03-07'
    coordinates = [[[43.17626953125, 39.70718665682654], [46.153564453125, 39.70718665682654],
                    [46.153564453125, 41.36856413680967], [43.17626953125, 41.36856413680967],
                    [43.17626953125, 39.70718665682654]]]

    myobj = {'computationURL': computationURL, 'nodes': nodes, 'dataRepositoryURL': dataRepositoryURL,
             'date_range': date_range, 'geometry': {'type': 'Polygon', 'coordinates': coordinates}}
    response = requests.post(url, json=myobj)
    time.sleep(1)
    return response.json()


df = pd.DataFrame(columns=['nodes', 'tiles', 'size', 'exec_time'])
try:
    for nodes in range(24, 16, -2):
        for tiles in range(16, 0, -1):
            result = execute(nodes, tiles)
            r = dict()
            r['nodes'] = nodes
            r['tiles'] = tiles
            r['size'] = np.sum([item.get('size') for item in result])
            r['exec_time'] = np.sum([item.get('exec_time') for item in result])
            df = df.append(r, ignore_index=True)
            df.to_csv('result_1.csv', index=False)
except Exception as e:
    print(e)
finally:
    df.to_csv('result_1.csv', index=False)

print("successfully finished!")

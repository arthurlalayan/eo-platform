import json
import os
import zipfile
from io import BytesIO

import earthpy.plot as ep
from fastapi.responses import StreamingResponse


def create_figure(result, filename):
    r = ep.plot_bands(result, cmap="RdYlGn", vmin=-1, vmax=1)
    r.get_figure().savefig(filename, bbox_inches='tight', pad_inches=0)


def zip_files(task_id):
    io = BytesIO()
    zip_filename = "%s.zip" % f"output_{task_id}"
    filenames = os.listdir(os.path.join('output', task_id))
    with zipfile.ZipFile(io, mode='w', compression=zipfile.ZIP_DEFLATED) as zip:
        for fpath in filenames:
            zip.write(os.path.join(os.getcwd(), 'output', task_id, fpath), arcname=fpath)
        zip.close()
    return StreamingResponse(
        iter([io.getvalue()]),
        media_type="application/x-zip-compressed",
        headers={"Content-Disposition": f"attachment;filename=%s" % zip_filename}
    )


def get_bbox(geometry):
    x_coordinates, y_coordinates = zip(*geometry['coordinates'][0])

    return [min(x_coordinates), min(y_coordinates), max(x_coordinates), max(y_coordinates)]

# def get_file_path(filename):
#     return f'../data/{filename}'

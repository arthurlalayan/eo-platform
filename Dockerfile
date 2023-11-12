FROM python:3.10.8-slim-buster

WORKDIR /code

RUN pip install --upgrade pip
COPY ./custom_libs/GDAL-3.4.3-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /code/lib/
RUN pip install /code/lib/GDAL-3.4.3-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
RUN rm -r /code/lib

COPY ./requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

COPY ./ /code/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]

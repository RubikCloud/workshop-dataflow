import base64
from io import BytesIO
import json
import os
from apache_beam import io
from apache_beam.io import gcsio
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions
from apache_beam.pipeline import Pipeline
import random
from datetime import datetime
import logging
import json
from apache_beam import (
    DoFn,
    GroupByKey,
    io,
    ParDo,
    PTransform,
    WindowInto,
    WithKeys,
)
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from time import time
from os import environ
from typing import List
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from matplotlib import colormaps
from math import cos, asin, sqrt
import numpy as np

### CONSTANTES
GCP_PROJECT_ID = "dataflow-workshop-rubik"
GCP_REGION = "us-central1"
GCS_BUCKET_URI = "gs://apachetmp/"
STAGING_BUCKET = "gs://tmp-workshop-staging"
WINDOW_SIZE = 30
GRAPH_CMAP = "Dark2"
PUBSUB_TOPIC = "test-apache-beam"
### END CONSTANTES
### HELPERS
TEMPS_BY_CITY_IN_MEMORY = {}


def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    hav = (
        0.5
        - cos((lat2 - lat1) * p) / 2
        + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    )
    return 12742 * asin(sqrt(hav))


def closest(v):
    return min(
        city_list, key=lambda p: distance(v["lat"], v["lon"], p["lat"], p["lon"])
    )


def draw_map(coords):
    plt.close()
    map = Basemap(projection="cyl", resolution="l")
    map.drawcoastlines(linewidth=0.25)
    map.drawcountries(linewidth=0.25)
    map.fillcontinents()
    map.drawmapboundary(fill_color="aqua")
    map.drawmeridians(np.arange(0, 360, 30))
    map.drawparallels(np.arange(-90, 90, 30))

    for p in coords:
        map.scatter(p["lon"], p["lat"], marker="o", color="r", zorder=5)
        plt.text(p["lon"], p["lat"], p["city"])

    return plt


def get_average(coords):
    return sum([x["temperature"] for x in coords]) / len(coords)


def draw_diagram(coords):
    global TEMPS_BY_CITY_IN_MEMORY
    for k in TEMPS_BY_CITY_IN_MEMORY:
        if len(TEMPS_BY_CITY_IN_MEMORY[k]) > 100:
            TEMPS_BY_CITY_IN_MEMORY = {}
            break
    cmap = colormaps[environ["GRAPH_CMAP"]]
    for c in coords:
        c["publish_time"] = datetime.strptime(c["publish_time"], "%Y-%m-%d %H:%M:%S.%f")
        if c["city"] not in TEMPS_BY_CITY_IN_MEMORY:
            TEMPS_BY_CITY_IN_MEMORY[c["city"]] = [
                {"temp": c["temperature"], "time": c["publish_time"]}
            ]
        else:
            TEMPS_BY_CITY_IN_MEMORY[c["city"]].append(
                {"temp": c["temperature"], "time": c["publish_time"]}
            )

    plt.close()
    plt.figure(figsize=(10, 6))
    i = 0
    for city in TEMPS_BY_CITY_IN_MEMORY:
        i += 1
        time = [x["time"] for x in TEMPS_BY_CITY_IN_MEMORY[city]]
        temperature = [x["temp"] for x in TEMPS_BY_CITY_IN_MEMORY[city]]

        plt.plot(time, temperature, label=city, color=cmap(i))

    plt.xlabel("Time")
    plt.ylabel("Temperature")
    plt.title("Temperatura sobre tiempo en ciudades")
    plt.legend()
    return plt


city_list = [
    {"city": "Buenos Aires", "lat": -34.61315, "lon": -58.37723},
    {"city": "Londres", "lat": 51.50853, "lon": -0.12574},
    {"city": "Jakarta", "lat": -6.21462, "lon": 106.84513},
    {"city": "Nueva York", "lat": 40.7127837, "lon": -74.0059413},
    {"city": "Moscu", "lat": 55.75222, "lon": 37.61556},
]


def city_to_temperature(city):
    var = 2
    avg = 10

    if city == "Buenos Aires":
        avg = 18
    elif city == "Londres":
        avg = 5
    elif city == "Jakarta":
        avg = 30
    elif city == "Moscu":
        avg = 4
    elif city == "Nueva York":
        avg = 10

    return (random.random() * var * 2 - var) + avg


def randomPointWithTimestamp():
    return {
        "lat": random.random() * 180 - 90,
        "lon": random.random() * 360 - 180,
        "timestamp": time() + (random.random() * 200 - 100),
    }


def randomPoint():
    return {"lat": random.random() * 180 - 90, "lon": random.random() * 360 - 180}


def save_and_encode_img(p):
    pic_bytes = BytesIO()
    p.savefig(pic_bytes, format="png")
    p.close()
    pic_bytes.seek(0)
    b64 = base64.b64encode(pic_bytes.read())
    return b64


def decode_img(b):
    pic_bytes = base64.b64decode(b)
    return pic_bytes


### END HELPERS
### TRANSFORMS Y FUNCIONES BEAM
class GroupMessagesByFixedWindows(PTransform):
    def __init__(self):
        self.window_size = int(WINDOW_SIZE)
        self.num_shards = 1

    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )


class WriteToGCS(DoFn):
    def process(self, key_value):
        """Write messages in a batch to Google Cloud Storage."""

        filename, b64pic = key_value

        with gcsio.GcsIO().open(filename=GCS_BUCKET_URI + filename, mode="w") as f:
            pic_bytes = decode_img(b64pic)
            f.write(pic_bytes)
            logging.info("File saved to GCS")


class DrawMap(DoFn):
    def process(self, key_value, window=DoFn.WindowParam):
        shard_id, batch = key_value

        filename = (
            "current.png"
            # "-".join([window_start, window_end]) + ".png"
        )

        coords = []
        for message_body, publish_time in batch:
            coord = json.loads(message_body)

            coord["publish_time"] = publish_time
            coord["city"] = closest(coord)["city"]
            coord["temperature"] = city_to_temperature(coord["city"])
            coords.append(coord)

        plt = draw_diagram(coords)
        pic_bytes = save_and_encode_img(plt)

        yield (filename, pic_bytes)


### END TRANSFORMS
### PIPELINE
def run():
    pipeline_options = PipelineOptions(streaming=True, save_main_session=True)

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = GCP_PROJECT_ID
    google_cloud_options.region = GCP_REGION
    google_cloud_options.staging_location = STAGING_BUCKET

    with Pipeline(options=pipeline_options) as pipeline:
        pipeline | (
            "Read from Pub/Sub"
            >> io.ReadFromPubSub(
                topic=f"projects/{GCP_PROJECT_ID}/topics/{PUBSUB_TOPIC}",
            )
            | "Window into" >> GroupMessagesByFixedWindows()
            | "Draw map" >> ParDo(DrawMap())
            | "Write to GCS" >> ParDo(WriteToGCS())
        )


if __name__ == "__main__":
    run()

# from perlin_noise import PerlinNoise
from random import random
from time import time, sleep
from typing import Dict, List, Union
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from math import cos, asin, sqrt
import numpy as np


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
    map = Basemap(projection="cyl", resolution="l")
    # draw coastlines, country boundaries, fill continents.
    map.drawcoastlines(linewidth=0.25)
    map.drawcountries(linewidth=0.25)
    map.fillcontinents(color="coral", lake_color="aqua")
    # draw the edge of the map projection region (the projection limb)
    map.drawmapboundary(fill_color="aqua")
    # draw lat/lon grid lines every 30 degrees.
    map.drawmeridians(np.arange(0, 360, 30))
    map.drawparallels(np.arange(-90, 90, 30))
    for p in coords:
        # set up orthographic map projection with
        # perspective of satellite looking down at 45N, 100W.
        # use low resolution coastlines.
        map.scatter(p["lon"], p["lat"], marker="o", color="r", zorder=5)
        plt.text(p["lon"], p["lat"], p["city"])

    plt.show()


city_list = [
    {"city": "Buenos Aires", "lat": -34.61315, "lon": -58.37723},
    {"city": "Londres", "lat": 51.50853, "lon": -0.12574},
    {"city": "Jakarta", "lat": -6.21462, "lon": 106.84513},
    {"city": "Nueva York", "lat": 40.7127837, "lon": -74.0059413},
    {"city": "Moscú", "lat": 55.75222, "lon": 37.61556},
]


def city_to_temperature(city: str):
    avg = 10

    if city == "Buenos Aires":
        avg = 12
    elif city == "Londres":
        avg = 10
    elif city == "Jakarta":
        avg = 10
    elif city == "Moscú":
        avg = 10
    elif city == "Nueva York":
        avg = 10

    return random() * avg


def randomPoint():
    return {"lat": random() * 180 - 90, "lon": random() * 360 - 180}

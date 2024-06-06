from argparse import ArgumentParser
import json
from time import time
from helpers import city_to_temperature, closest, randomPoint


def mock_processed(n):
    points = []
    for i in range(n):
        p = randomPoint()
        p["publish_time"] = time() + i * 10000
        p["city"] = closest(p)["city"]
        p["temperature"] = city_to_temperature(p["city"])
        points.append(p)

    with open("mock_processed.json", "w") as f:
        json.dump(points, f)


parser = ArgumentParser()
parser.add_argument("--n", default=100)

if __name__ == "__main__":
    args = parser.parse_args()
    mock_processed(args.n)

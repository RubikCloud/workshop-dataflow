# import ndjson
import json
from random import random
from time import sleep
from dotenv import load_dotenv
from base64 import b64encode
from main import randomPoint
import argparse
import httpx
import os

load_dotenv()

r = httpx.Client(http2=True)


def ping(token):
    EMULATOR = not bool(token)

    LOCALHOST_URL = "http://localhost:8043"
    BASE_URL = LOCALHOST_URL if EMULATOR else "https://pubsub.googleapis.com"

    PROJECT = "dataflow-workshop-rubik"  # os.environ["GCP_PROJECT_ID"]
    TOPIC = os.environ["PUBSUB_TOPIC"]
    SUBSCRIPTION = "mysub"

    CREATE_TOPIC_URL = f"{BASE_URL}/v1/projects/{PROJECT}/topics/{TOPIC}"
    CREATE_SUBSCRIPTION_URL = (
        f"{BASE_URL}/v1/projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"
    )
    PUSH_URL = f"{BASE_URL}/v1/projects/{PROJECT}/topics/{TOPIC}:publish"

    print(PUSH_URL)

    if EMULATOR:
        try:
            t_res = r.put(CREATE_TOPIC_URL, headers={})
            print(t_res.json())
        except httpx.ConnectError:
            raise Exception(
                "El servidor local no está corriendo. \n"
                + f"gcloud beta emulators pubsub start --project={PROJECT} --host-port='{LOCALHOST_URL}'",
            )
        s_res = r.put(
            CREATE_SUBSCRIPTION_URL,
            json={
                "topic": f"projects/{PROJECT}/topics/{TOPIC}",
                "pushConfig": {
                    "pushEndpoint": "http://localhost:8080/projects/{PROJECT}/topics/{TOPIC}"
                },
            },
        )
        print(s_res.json())

    count = 0
    while True:
        sleep(random() * 5)

        coord = randomPoint()
        data = b64encode(json.dumps(coord).encode()).decode()
        res = r.post(
            PUSH_URL,
            json={"messages": [{"data": data}]},
            headers={} if EMULATOR else {"Authorization": f"Bearer {token}"},
        )

        if "messageIds" in res.json():
            count += 1
            print(f"sent {count} messages", end="\r")


parser = argparse.ArgumentParser()
parser.add_argument(
    "--token",
    required=False,
    default=None,
    help="gcloud auth application-default print-access-token",
)

if __name__ == "__main__":
    args = parser.parse_args()
    ping(args.token)
    ## python ping.py --token=$(gcloud auth application-default print-access-token)

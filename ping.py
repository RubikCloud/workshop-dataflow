import json
from random import random
from time import sleep
from requests import request
from base64 import b64encode
from helpers import randomPoint

# gcloud beta emulators pubsub start --project=tetetetest --host-port='localhost:8043'

PROJECT = "tetetetest"
TOPIC = "mytopic"
SUBSCRIPTION = "mysub"

CREATE_TOPIC_URL = f"http://localhost:8043/v1/projects/{PROJECT}/topics/{TOPIC}"
CREATE_SUBSCRIPTION_URL = (
    f"http://localhost:8043/v1/projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"
)
PUSH_URL = f"http://localhost:8043/v1/projects/{PROJECT}/topics/{TOPIC}:publish"


def ping():
    request("PUT", CREATE_TOPIC_URL)
    request(
        "PUT",
        CREATE_SUBSCRIPTION_URL,
        json={
            "topic": f"projects/{PROJECT}/topics/{TOPIC}",
            "pushConfig": {
                "pushEndpoint": "http://localhost:8080/projects/{PROJECT}/topics/{TOPIC}"
            },
        },
    )

    while True:
        sleep(random() * 0.2)

        coord = randomPoint()
        data = b64encode(json.dumps(coord).encode()).decode()
        request(
            "POST",
            PUSH_URL,
            json={"messages": [{"data": data}]},
        )


if __name__ == "__main__":
    ping()

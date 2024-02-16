import json
import os
import apache_beam as beam
from apache_beam import io
from apache_beam.io.gcp import pubsub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.pipeline import Pipeline
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime
from helpers import closest, city_list, city_to_temperature

os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8043"


class AddTimestampDoFn(beam.DoFn):
    def process(self, element: io.PubsubMessage):
        unix_timestamp = element.publish_time.timestamp()
        print(unix_timestamp)

        yield window.TimestampedValue(element, unix_timestamp)


class StringToCoord(beam.DoFn):
    def process(self, message: io.PubsubMessage):
        print(message)
        coord = json.loads(message.data)
        yield coord


class CoordToCity(beam.DoFn):
    def process(self, coord):
        coord["city"] = closest(coord)["city"]
        yield coord


class CityToTemperature(beam.DoFn):
    def process(self, point_with_city):
        point_with_city["temperature"] = city_to_temperature(point_with_city["city"])
        yield point_with_city


def MapFn(points):
    print("p:", points, len(points))


def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    with Pipeline(options=pipeline_options) as p:
        lines = p | "Read from Pub/Sub" >> io.ReadFromPubSub(
            "projects/tetetetest/topics/mytopic",
            with_attributes=True,
        )
        timestamped_lines = (
            lines
            | "Apply timestamp" >> beam.ParDo(AddTimestampDoFn())
            | "Apply windowing"
            >> beam.WindowInto(
                window.FixedWindows(5),
                # trigger=AfterProcessingTime(15),
                # accumulation_mode=AccumulationMode.DISCARDING,
            )
        )

        points = (
            timestamped_lines
            | "Pub/Sub message to coordinate" >> beam.ParDo(StringToCoord())
            | "Add closest city to coordinate" >> beam.ParDo(CoordToCity())
            | "Add temperature of closest city to coordinate"
            >> beam.ParDo(CityToTemperature())
            | beam.Map(MapFn)
        )

        # graphs = points | beam.Map(MapFn)


if __name__ == "__main__":
    run()

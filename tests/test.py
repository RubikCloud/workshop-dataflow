from typing import List
import apache_beam as beam
from apache_beam.pipeline import Pipeline
from apache_beam.transforms.window import (
    TimestampedValue,
    Sessions,
)
from apache_beam.utils.timestamp import Duration
from apache_beam.io.textio import WriteToText, ReadFromText

branched = Pipeline()
input_collection = (
    branched
    | "Read from text file" >> ReadFromText("data.txt")
    | "Split rows" >> beam.Map(lambda line: line.split(","))
    | "Clean rows" >> beam.Map(lambda line: [l.strip() for l in line])
)

input_collection | beam.Map(print)

backend_dept = (
    input_collection
    | "Retrieve Backend employees" >> beam.Filter(lambda record: record[3] == "Backend")
    | "Pair them 1–1 for Backend"
    >> beam.Map(lambda record: ("Backend, " + record[1], 1))
    | "Aggregation Operations: Grouping & Summing1" >> beam.CombinePerKey(sum)
)
ai_dept = (
    input_collection
    | "Retrieve AI employees" >> beam.Filter(lambda record: record[3] == "AI")
    | "Pair them 1–1 for HR" >> beam.Map(lambda record: ("AI, " + record[1], 1))
    | "Aggregation Operations: Grouping & Summing2" >> beam.CombinePerKey(sum)
)

output = (backend_dept, ai_dept) | beam.Flatten() | WriteToText("branched.txt")
branched.run()

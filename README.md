# workshop-dataflow

### De qué trata:

- montar Dataflow Streaming para recibir datos desde un tópico de Pub/Sub, guardarlos en BigQuery y mostrarlos en Looker.

### Qué APIs se usan:

- Dataflow

- BigQuery

- Pub/Sub

### Requisitos, en caso de tener:

- No

### Flow:

- Ping desde local con "temperatura de cada lugar"
- Dataflow (en modo streaming).
- Windowing de 30 segundos, guarda en una DB el promedio de todas esas temperaturas.

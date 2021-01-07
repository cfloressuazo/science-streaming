import faust

KAFKA_BOOTSTRAP_SERVER = "kafka://localhost:9092"

app = faust.App(
    'faust_project',
    version=1,
    autodiscover=True,
    origin='faust_project',
    broker=KAFKA_BOOTSTRAP_SERVER,
    topic_partitions=4,
    broker_max_poll_records=500,
)


def main() -> None:
    app.main()

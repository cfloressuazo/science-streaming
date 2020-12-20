import faust

KAFKA_BOOTSTRAP_SERVER = "kafka://localhost:9092"

app = faust.App(
    'faust_project',
    version=1,
    autodiscover=True,
    origin='faust_project',
    broker=KAFKA_BOOTSTRAP_SERVER
)


def main() -> None:
    app.main()

# Setup Environment

1. Build up kafka environment in detach mode `-d` (optional)
```bash
docker-compose up --build
```
2. Set up the connector for Kafka Connect to pick up CSV files and load the raw data into a Kafka topic.
```bash
curl -i -X PUT -H "Accept:application/json" \
    -H  "Content-Type:application/json" http://localhost:8083/connectors/source-csv-spooldir-00/config \
    -d @producers/schemas/config.json
```
- connector name: `source-csv-spooldir-00`
- topic name: `org.science.medicare`
- schema: `producers/schemas/simple_config.json`

3. Download the raw data into the unprocessed data folder.
```bash
wget 
```
4. Set up event producer to simulate incoming events
```bash
cd producers
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

5. Set up event consumers for real-time analytics
```bash
cd consumers
python3 -m venv venv
. venv/bin/activate
python3 setup.py develop
```

# Run medicare event simulation
Run the following to simulate an infinite stream of events into a `org.science.medicare` kafka topic.

**Notes**:
- make sure you have activated the __producers__ virtual environment before running this command - (step 4).
- make sure the raw data is available for this script to load it. (processed or unprocessed folder)
```bash
# from producers directory
python3 medicare_simulator.py
```
This script will run in an infinite loop until you cancel by keyboard `CTRL + C`.
For simulation purposes, leave it run it in a separate terminal window.

# Run Real-time analytics
Run the following to print the analytics in real-time of the events that are being sent into
a kafka topic.

**Notes**:
- make sure you have activated the __consumers__ virtual environment before running these commands.
- make sure you have docker running (`docker-compose ps` on the root of the project).
- make sure you have the event simulation running.
```bash
consumers worker -l info
```

# Extras:
__Create a clean environment__:
```bash
docker-compose rm -f
docker-compose pull
docker-compose up --build
```

__Delete connectors__:
```bash
curl -s "http://localhost:8083/connectors" | \
        jq  '.[]' | \
        peco | \
        xargs -I{connector_name} curl -s -XDELETE "http://localhost:8083/connectors/"\{connector_name\}
```

__List existing topics__:
```bash
docker exec kafkacat kafkacat -b kafka:29092 -L -J|jq '.topics[].topic'|sort
```
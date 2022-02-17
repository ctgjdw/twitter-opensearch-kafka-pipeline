# Twitter API

This repository contains the python producer (integrated with a simple `Twitter API`) to stream data into Opensearch via `Kafka`. The setup also utilises the Confluent Schema Registry to enforce a versioned schema onto the entire pipeline.

## Twitter Keys

The Twitter keys should be loaded into a `.env` file in the `root` directory. The keys required are:

```
TWITTER_API_TOKEN='<TWITTER_BEARER_TOKEN>'
TWITTER_API_KEY='<TWITTER_KEY>'
TWITTER_API_SECRET='<TWITTER_SECRET>'
```

## Usage

- To produce from Twitter api run:
```bash
python main.py
```

- To consume and index to `OpenSearch` run:
```bash
cd kafka
python kafka_consumer.py
```

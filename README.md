# Real-Time IoT Data Pipeline

A real-time data ingestion pipeline: a producer streams data to AWS IoT Core,
which triggers a Lambda function that persists each record to a relational
database.

## Architecture

```
data_producer/producer.py → AWS IoT Core → lambda_function/lambda_function.py → RDS
```

- **`producer.py`** — generates and publishes data in real time over MQTT,
  authenticated with a device certificate.
- **AWS IoT Core** — receives the stream and routes it to Lambda via an IoT
  rule.
- **`lambda_function.py`** — an AWS Lambda function that writes each incoming
  record to Amazon RDS.

## Tech Stack

Python, AWS IoT Core, AWS Lambda, Amazon RDS, MQTT, X.509 device
certificates.

## Note

This was built as a coursework project on an AWS account that has since been
closed; the pipeline is not currently deployed. Device certificates in this
repo are inert (the account no longer exists) and are kept for reference
only — `certs/*-private.pem.key` is git-ignored going forward regardless.

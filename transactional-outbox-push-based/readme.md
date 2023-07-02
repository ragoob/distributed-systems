## What is the problem 
How to atomically update the database and send messages to a message broker?

## Solution
The solution is for the service that sends the message to first store the message in the database as part of the transaction that updates the business entities. A separate process or thread watch transactional WAL data (database write ahead log) with logical replication and send the message to the message broker then Send Stand by Status Update to flush the WAL after message successfuly sent to MQ

## Technolgies
 Postgres db 

## Drawbacks
- Not all database support this (require database specific solution)
- At least once idempotancy so you need to add some deduplication logic

## Design
![Alt Text](outbox.png)

## Run 
```
docker-compose up -d
CREATE DATABASE YOUR_DB_NAME;
CREATE TABLE outbox (
   position    BIGSERIAL     PRIMARY KEY,
   event_type  VARCHAR(250)  NOT NULL,
   data        JSONB         NOT NULL
);
CREATE PUBLICATION outbox_pub FOR TABLE outbox;
SELECT * FROM pg_create_logical_replication_slot('outbox_slot', 'pgoutput');

OUTBOX_DEMO_CONN_STRING="postgres://postgres:postgres@127.0.0.1/order_service?replication=database" go run main.go
```

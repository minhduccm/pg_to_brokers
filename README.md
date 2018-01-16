# pg_to_brokers
## Inspiration
In modern and sophisticated architecture/application, the requirement like copying database contents to some other system in near realtime and transaction commit time order guarantee is becoming more popular. For example, to make it searchable in ElasticSearch or to load it into a data warehouse for analytics. With those kinds of task, there is no room for taking snapshot of database approach.

One option to achive this is to do something like "dual writes". That is, every time your application code writes to database, it also do additional action against external system (sending changes to data warehouse and trigger re-compute scores for recommendation system with those change, for example)

This approach looks simple but it could lead to data inconsistency problem - Data between 2 different datastores will become more and more inconsistent overtime because of bugs, server downs, ...

## Introduction
**pg_to_brokers** is a lightweight library to stream continuously changes from PostgreSQL database to popular streaming brokers such as AWS Kinesis, Apache Kafka, etc... (currently just supported Kinesis)
It's Python lib that utilises logical decoding feature of PostgreSQL (>= 9.4) to capture changes from Write Ahead Log (WAL) then publish them to broker - a.k.a Kinesis for now with:

**extendability** - you'll have fully control in hand with your custom code

**scalability** - you can create multiple Kinesis shards for parallel processing

**reliability** - ensure no data will be lost on any sort of system failure (process crashed, network outages, ...)

## Installation

## Usage

## Notes:
- With fault tolerant design in mind, we guarantee no data lost but in case of system failure that could lead to sending messages/events more than once so your Consumer workers should be able to handle duplication as well.

## Future works:

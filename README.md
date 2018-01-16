# pg_to_brokers
## Inspiration
In modern and sophisticated architecture/application, the requirement like copying database contents to some other system in near realtime and transaction commit time order guarantee is becoming more popular. For example, to make it searchable in ElasticSearch or to load it into a data warehouse for analytics. With those kinds of task, there is no room for taking snapshot of database approach.

One option to achive this is to do something like "dual writes". That is, every time your application code writes to database, it also do additional action against external system (sending changes to data warehouse and trigger re-compute scores for recommendation system with those change, for example)

This approach looks simple but it could lead to data inconsistency problem - Data between 2 different datastores will become more and more inconsistent overtime because of bugs, server downs, ...

![alt text](https://cdn2.hubspot.net/hub/540072/file-3062873213-png/blog-files/slide-37-4-3.png "Stop do this")

## Introduction
**pg_to_brokers** is a lightweight library to stream continuously changes from PostgreSQL database to popular streaming brokers such as AWS Kinesis, Apache Kafka, etc... (currently just supported Kinesis)
It's Python lib that utilises [logical decoding](https://www.postgresql.org/docs/9.4/static/logicaldecoding.html) feature of PostgreSQL (>= 9.4) to capture changes from Write Ahead Log (WAL) then publish them to broker - a.k.a Kinesis for now with:

**extendability** - you'll have fully control in hand with your custom code

**scalability** - you can create multiple Kinesis shards for parallel processing

**reliability** - ensure no data will be lost on any sort of system failure (process crashed, network outages, ...)

![alt text](https://github.com/minhduccm/pg_to_brokers/blob/master/images/architecture.png "Architecture")

## Installation
1. Update PostgreSQL configuration to enable Logical Decoding feature:
* ```wal_level``` to ```logical```
* ```max_replication_slots``` to at least 1
2. Set up AWS Kinesis stream
3. Install: ```pip install pg_to_brokers```

## Usage (Examples)
1. Producer: 
* [kinesis_stream_producer](https://github.com/minhduccm/pg_to_brokers/blob/master/examples/kinesis_stream_producer.py)
* [kinesis_stream_producer_with_dynamic_partition_key](https://github.com/minhduccm/pg_to_brokers/blob/master/examples/kinesis_stream_producer_with_dynamic_partition_key.py)
2. Consumer:
* [kinesis_stream_consumer](https://github.com/minhduccm/pg_to_brokers/blob/master/examples/kinesis_stream_consumer.py)
3. Record format:
```
{
    fields: <array of field of the change>,
    values: <array of value of the change. Sorted respectively by fields' order>,
    types: <array of type of change values. Sorted respectively by values' order>,
    table_name: <string - table name>,
    operation: <string - would be INSERT/UPDATE/DELETE>,
    transaction_id: <string>
}
```

## Notes
With fault tolerant design in mind, we guarantee no data lost. But in case of system failure that could lead to sending messages/events more than once so your Consumer workers should be able to handle duplication as well.

## Future works
- Support Apache Kafka

storm-applications
==================

A collection of real-time applications built with Apache Storm.

## Applications

| Application Name      | Prefix | Sample Data              | Dataset
|-----------------------|--------|--------------------------|--------
| ads-analytics         | aa     | [ad-clicks.dat][13]      | [KDD Cup 2012][8] (12GB)
| bargain-index         | bi     |                          | [Yahoo Finance][2], [Google Finance][3]
| click-analytics       | ca     | [click-stream.json][14]  | [1998 WorldCup][7] (104GB)
| fraud-detection       | fd     | [credit-card.dat][15]    |
| linear-road           | lr     |                          |
| log-processing        | lp     | [http-server.log][16]    | [1998 WorldCup][7] (104GB)
| machine-outlier       | mo     | [cluster-traces.csv][17] | [Google Cluster Traces][6] (36GB)
| reinforcement-learner | rl     |                          |
| sentiment-analysis    | sa     |                          | [Twitter Streaming][5]
| spam-filter           | sf     | [enron.json][18]         | [TREC 2007][9] (547MB, labeled)<br />[SPAM Archive][10] (~1.2GB, spam)<br />[Enron Email Dataset][11] (2.6GB, raw)<br />[Enron Spam Dataset][12] (50MB, labeled)
| spike-detection       | sd     | [sensors.dat][19]        | [Intel Berkeley Research Lab][4] (150MB)
| traffic-monitoring    | tm     | [taxi-traces.csv][22]    | [Beijing Taxi Traces][21]
| trending-topics       | tt     |                          | [Twitter Streaming][5]
| voipstream            | vs     |                          |
| word-count            | wc     | [books.dat][20]          | [Project Gutenberg][1] (~8GB)


## Usage

### Build

```bash
$ git clone git@github.com:mayconbordin/storm-applications.git
$ cd storm-applications/
$ mvn -P<profile> package
```

Use the `local` profile to run the applications in local mode or `cluster` to run in a remote cluster.

### Submit a Topology

Syntax:

```bash
$ bin/storm <jar> <application-name> (local|remote) [OPTIONS...]
```

Example:

```bash
$ bin/storm target/storm-applications-*-with-dependencies.jar word-count local
```

Options:

```bash
  --config=<file>        The configuration file.
  --runtime=<runtime>    Runtime in seconds (local mode only) [default: 300].
  --topology-name=<name> The name of the topology (remote mode only).
```

## Configuration

Instead of each application having its own spouts and sinks (bolts that send data to other systems), we have defined a few basic spouts and sinks.

### Spouts

All but the `GeneratorSpout` need a `Parser`. The parser receives a string and returns a list of values, following the schema defined in the topology. To set a spout that reads from a file and parses the data as a Common Log Format, the configuration file would look like this:

```
<app-prefix>.spout.threads=1
<app-prefix>.spout.class=storm.applications.spout.BufferedReaderSpout
<app-prefix>.spout.path=./data/http-server.log
<app-prefix>.spout.parser=storm.applications.spout.parser.CommonLogParser
```

Defalult parsers:

| Parse                    | Output Fields
|--------------------------|--------------------
| AdEventParser            | (quer_id, ad_id, event) 
| BeijingTaxiTraceParser   | (car_id, date, occ, speed, bearing, lat, lon)
| ClickStreamParser        | (ip, url, client_key)
| CommonLogParser          | (ip, timestamp, minute, request, response, byte_size)
| GoogleTracesParser       | (timestamp, id, cpu, memory)
| JsonEmailParser          | (id, message[, is_spam])
| JsonParser               | (json_object)
| SensorParser             | (id, timestamp, value)
| StringParser             | (string)
| TransactionParser        | (event_id, actions)


#### GeneratorSpout

The `GeneratorSpout` doesn't need a parser, instead it uses an instance of a class that extends the `Generator` class. Each time the generator is called it returns a new tuple.

```
<app-prefix>.spout.threads=1
<app-prefix>.spout.class=storm.applications.spout.GeneratorSpout
<app-prefix>.spout.generator=storm.applications.spout.generator.SensorGenerator
```

Defalult generators:

| Generator                | Configurations
|--------------------------|------------------------------------------------------
| CDRGenerator             | `vs.generator.population`<br />`vs.generator.error_prob`
| MachineMetadataGenerator | `mo.generator.num_machines`
| RandomSentenceGenerator  | 
| SensorGenerator          | `sd.generator.count`

#### KafkaSpout

```
<app-prefix>.kafka.zookeeper.host=
<app-prefix>.kafka.spout.topic=
<app-prefix>.kafka.zookeeper.path=
<app-prefix>.kafka.consumer.id=
```

#### RedisSpout

```
<app-prefix>.redis.server.host=
<app-prefix>.redis.server.port=
<app-prefix>.redis.server.pattern=
<app-prefix>.redis.server.queue_size=
```

#### TwitterStreamingSpout

```
<app-prefix>.twitter.consumer_key=
<app-prefix>.twitter.consumer_secret=
<app-prefix>.twitter.access_token=
<app-prefix>.twitter.access_token_secret=
```

### Sinks

Similarly, some sink classes need a `Formatter` which receives a tuple and returns a string. Writing the output of an application to a file could be achieved with the following configuration:

```
<app-prefix>.sink.threads=1
<app-prefix>.sink.class=storm.applications.sink.FileSink
<app-prefix>.sink.path=./output/result_%(taskid).dat
<app-prefix>.sink.formatter=storm.applications.sink.formatter.FullInfoFormatter
```

The `<app-prefix>` is the prefix of the application being executed, and the placeholder `%(taskid)` is used in the file path so that each instance of the `FileSink` can write to its own file, avoiding problems in case of two instances residing in the same machine.

Defalult formatters:

| Formatter                | Format
|--------------------------|------------------------------------------------------
| ActionFormatter          | `<event_id>,<actions>`
| BasicFormatter           | `<field>=<value>, ...`
| FullInfoFormatter        | `source: <name>:<id>, stream: <name>, id: <id>, values: [<field>=<value>, ...]`
| MachineMetadataFormatter | `<anomaly_stream>, <anomaly_score>, <timestamp>, <is_abnormal>, <cpu_idle>, <mem_free>`

#### CassandraBatchSink

```
<app-prefix>.cassandra.host=
<app-prefix>.cassandra.keyspace=
<app-prefix>.cassandra.sink.column_family=
<app-prefix>.cassandra.sink.field.row_key=
<app-prefix>.cassandra.sink.ack_strategy=
```

#### CassandraCountBatchSink

```
<app-prefix>.cassandra.host=
<app-prefix>.cassandra.keyspace=
<app-prefix>.cassandra.sink.column_family=
<app-prefix>.cassandra.sink.field.increment=
<app-prefix>.cassandra.sink.ack_strategy=
```

#### RedisSink

```
<app-prefix>.redis.server.host=
<app-prefix>.redis.server.port=
<app-prefix>.redis.sink.queue=
```

#### SocketSink

```
<app-prefix>.sink.socket.port=
<app-prefix>.sink.socket.charset=
```

## Metrics

By using hooks (`ITaskHook`) and the [metrics](http://metrics.codahale.com/) library it is possible to collect performance metrics of bolts and spouts. In bolts information is collected about the number of received and emitted tuples and the execution time, while for spouts information about the complete latency and emitted tuples is recorded.
To enable metric collection, use the following configuration:

```
metrics.enabled=true
metrics.reporter=csv
metrics.interval.value=2
metrics.interval.unit=seconds
metrics.output=/tmp
```

The available reporters are `csv`, `console` and `slf4j`, but only the `csv` needs the `metrics.output` configuration, which defaults to `/tmp`.

[1]: http://www.gutenberg.org/
[2]: https://finance.yahoo.com/
[3]: https://www.google.com/finance
[4]: http://db.csail.mit.edu/labdata/labdata.html
[5]: https://dev.twitter.com/docs/api/streaming
[6]: http://code.google.com/p/googleclusterdata/
[7]: http://ita.ee.lbl.gov/html/contrib/WorldCup.html
[8]: http://www.kddcup2012.org/c/kddcup2012-track2/data
[9]: http://plg.uwaterloo.ca/~gvcormac/spam/
[10]: http://untroubled.org/spam/
[11]: http://www.cs.cmu.edu/~./enron/
[12]: http://nlp.cs.aueb.gr/software_and_datasets/Enron-Spam/index.html

[13]: data/ad-clicks.dat
[14]: data/click-stream.json
[15]: data/credit-card.dat
[16]: data/http-server.log
[17]: data/cluster-traces.csv
[18]: data/enron.json
[19]: data/sensors.dat
[20]: data/books.dat

[21]: http://anrg.usc.edu/www/downloads/
[22]: data/taxi-traces.csv

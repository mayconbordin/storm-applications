storm-applications
==================

A collection of real-time applications built with Apache Storm.

## Table of Contents

* [Applications](#applications)
* [Usage](#usage)
    * [Build](#build)
    * [Submit a Topology](#submit-a-topology)
* [Conventions for Building Applications](#conventions-for-building-applications)
    * [Application Prefix](#application-prefix)
    * [Constants](#constants)
    * [Spouts](#spouts)
    * [Bolts](#bolts)
    * [Sinks](#sinks)
    * [Topology-specific Code](#topology-specific-code)
* [Configuration](#configuration)
    * [Spouts](#configuration-spouts)
        * [GeneratorSpout](#configuration-generator-spout)
            * [SmartPlugGenerator](#configuration-smart-plug-generator)
        * [KafkaSpout](#configuration-kafka-spout)
        * [RedisSpout](#configuration-redis-spout)
        * [TwitterStreamingSpout](#configuration-twitter-streaming-spout)
    * [Sinks](#configuration-sinks)
        * [CassandraBatchSink](#configuration-cassandra-batch-sink)
        * [CassandraCountBatchSink](#configuration-count-batch-sink)
        * [RedisSink](#configuration-redis-sink)
        * [SocketSink](#configuration-socket-sink)
* [Metrics](#metrics)

## <a id="applications"></a>Applications

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
| smart-grid            | sg     | [smart-grid.csv][23]     | [DEBS 2014 Grand Challenge][24] (3.2GB)


## <a id="usage"></a>Usage

### <a id="build"></a>Build

```bash
$ git clone git@github.com:mayconbordin/storm-applications.git
$ cd storm-applications/
$ mvn -P<profile> package
```

Use the `local` profile to run the applications in local mode or `cluster` to run in a remote cluster.

If you are going to use the [storm](bin/storm) (do not confuse with the [`storm`](http://storm.apache.org/documentation/Command-line-client.html) command line client) script to submit topologies to a cluster, you must also install the python requirements:

```bash
$ pip install -r bin/requirements.txt
```

### <a id="submit-a-topology"></a>Submit a Topology

Syntax:

```bash
$ bin/storm-submit <jar> <application-name> (local|remote) [OPTIONS...]
```

Example:

```bash
$ bin/storm-submit target/storm-applications-*-with-dependencies.jar word-count local
```

Options:

```bash
  --config=<file>        The configuration file.
  --runtime=<runtime>    Runtime in seconds (local mode only) [default: 300].
  --topology-name=<name> The name of the topology (remote mode only).
```

## <a id="conventions-for-building-applications"></a>Conventions for Building Applications

Topologies are placed in the `storm.applications.topology` package and they must 
extend either the `AbstractTopology` or `BasicTopology` class. The `BasicTopology` 
class extends the `AbstractTopology` and assumes that the topology has only a single 
spout and sink components. In case your topology has multiple spouts and/or sinks 
you will have to extend the `AbstractTopology` class.

The `AbstractTopology` class defines four methods that must be implemented:

```java
public void initialize();
public StormTopology buildTopology();
public Logger getLogger();
public String getConfigPrefix();
```

In the `initialize` method you will load all the configuration variables, such as 
the parallelization hint for the bolts. As a convention, all the configuration that 
is going to be used within a bolt will be loaded in the initialization of the bolt,
instead of the topology.

The `buildTopology` method, as the name suggests, will use the `TopologyBuilder` 
class in order to wire the spouts and bolts together, returning an instance of 
`StormTopology`. The `getLogger` method is used in case the `AbstractTopology` 
has to report an error or warning.


### <a id="application-prefix"></a>Application Prefix

Each topology must also have an prefix which will identify the topology. The prefix 
is defined by returning it in the `getConfigPrefix` method and it will be used in 
the default configuration options defined at `BaseConstants.BaseConf`.

For example the `BaseConf.SPOUT_THREADS` variable has the value `%s.spout.threads`, 
which will be translated to `<app-prefix>.spout.threads` for a topology with the 
`<app-prefix>` prefix.

With these conventions defined, you can use the `loadSpout` and `loadSink` methods
from the `AbstractTopology` class. These methods will create an instance of a spout
or sink based on the class defined at `<app-prefix>.spout.class` and `<app-prefix>.sink.class`,
respectively.

If you have more than one spout (or sink), you can use the `loadSpout(String name)`
(or `loadSink(String name)`) method, which will create an instance of a spout class 
defined at `<app-prefix>.<name>.spout.class`.


### <a id="constants"></a>Constants

The basic constants used by all topologies are defined in the interface `BaseConstants`,
with one sub-interface for each type of constants, namely:

  - `BaseConf`: for configuration keys.
  - `BaseComponent`: for the name of components (spouts, bolts, sinks).
  - `BaseStream`: for the name of streams.

Each application will have its own constants interface at the `storm.applications.interfaces`
package, which will extend the `BaseConstants` interface. Each sub-interface will
also extend the sub-interfaces listed above.

Although the sub-interfaces may have any name, we recommend using: `Conf`, `Component` 
and `Stream`. Any other constant values that will be used topology-wide should be 
placed in the constants interface.

One kind of constant that must be placed in the interface is the name of the fields 
used by the components of a topology. Example for the word count topology:

```java
interface Field {
    String TEXT  = "text";
    String WORD  = "word";
    String COUNT = "count";
}
```

### <a id="spouts"></a>Spouts

Spouts are placed in the `storm.applications.spout` package and they must extend
the `AbstractSpout` class, which in turn extends the `BaseRichSpout` class.

The output fields of a bolt can be declared, instead of using the `declareOutputFields` 
method, by using the `setFields(Fields fields)` and `setFields(String streamId, Fields fields)` 
methods.

In order to enable an topology to retrieve data from different sources without the 
need to rewrite the whole spout, we have defined a few basic spouts that fetch the 
raw data from the source and hand it over to an implementation of the `Parser` interface.

In this way you can switch the source of data of a topology by simply changing the 
spout class in configuration file.

### <a id="bolts"></a>Bolts

Bolts are placed in the `storm.applications.bolt` package and they must extend
the `AbstractBolt` class, which in turn extends the `BaseRichBolt` class.

The output fields of a bolt can be declared, instead of using the `declareOutputFields` 
method, by using the `setFields(Fields fields)` and `setFields(String streamId, Fields fields)` 
methods.

You can also define the output fields inside the bolt class, by implementing one of 
these two methods:

```java
public Fields getDefaultFields();
public Map<String, Fields> getDefaultStreamFields();
```

The first method defines the fields for the default stream, while the second one 
defines the fields for multiple streams, whose names are defined as keys for the `Map`.

Instead of using the `prepare` method, you can override the `initialize` method, 
and the configuration, context and output collector objects can be accessed by the 
protected attributes `config`, `context` and `collector`, respectively.

### <a id="sinks"></a>Sinks

Sinks are placed in the `storm.applications.sink` package and they must extend
the `BaseSink` class, which in turn extends the `AbstractBolt` class.

A sink is nothing more than a bolt without output streams, i.e. they only consume 
data from streams.

In the same way as the spouts, we have implemented a few basic sinks that receive 
tuples from upstream bolts, hand it over to an implementation of the `Formatter` 
interface, and write the formatted tuple in the target sink (e.g. database, message system, queue).

### <a id="topology-specific-code"></a>Topology-specific Code

Any source-code regarding the logic of the application should be placed in an 
specific package inside the `storm.applications.model` package.

And if the source-code is just an utility, place it in the `storm.applications.util` 
package.

## <a id="configuration"></a>Configuration

Instead of each application having its own spouts and sinks (bolts that send data to other systems), we have defined a few basic spouts and sinks.

### <a id="configuration-spouts"></a>Spouts

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
| DublinBusTraceParser     | (car_id, date, occ, speed, bearing, lat, lon)
| GoogleTracesParser       | (timestamp, id, cpu, memory)
| JsonEmailParser          | (id, message[, is_spam])
| JsonParser               | (json_object)
| SensorParser             | (id, timestamp, value)
| SmartPlugParser          | (id, timestamp, value, property, plugId, householdId, houseId)
| StringParser             | (string)
| TransactionParser        | (event_id, actions)


#### <a id="configuration-generator-spout"></a>GeneratorSpout

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
| SmartPlugGenerator       | --


##### <a id="configuration-smart-plug-generator"></a>SmartPlugGenerator

The SmartPlugGenerator is an adaptation of a generator built by [Alessandro Sivieri][25]:

> Generates a dataset of a random set of smart plugs, each being part of a household, 
> which is, in turn, part of a house. Each smart plug records the actual load 
> (in Watts) at each second. The generated dataset is inspired by the DEBS 2014 
> challenge and follow a similar format, a sequence of 6 comma separated values 
> for each line (i.e., for each reading):
> 
>  - a unique identifier of the measurement [64 bit unsigned integer value]
>  - a timestamp of measurement (number of seconds since January 1, 1970, 00:00:00 GMT) [64 bit unsigned integer value]
>  - a unique identifier (within a household) of the smart plug [32 bit unsigned integer value]
>  - a unique identifier of a household (within a house) where the plug is located [32 bit unsigned integer value]
>  - a unique identifier of a house where the household with the plug is located [32 bit unsigned integer value]
>  - the measurement [32 bit unsigned integer]

This class generates smart plug readings at fixed time intervals, storing them 
into a queue that will be consumed by a `GeneratorSpout`.

The readings are generated by a separated thread and the interval resolutions is
of seconds. In order to increase the volume of readings you can decrease the 
interval down to 1 second. If you need more data volume you will have to tune
the other configuration parameters.

Configurations parameters:

  - `sg.generator.interval_seconds`: interval of record generation in seconds.
  - `sg.generator.houses.num`: number of houses in the scenario.
  - `sg.generator.households.min` and `sg.generator.households.max`: the range of number of households within a house.
  - `sg.generator.plugs.min` and `sg.generator.plugs.max`: the range of smart plugs within a household.
  - `sg.generator.load.list`: a comma-separated list of peak loads that will be randomly assigned to smart plugs.
  - `sg.generator.load.oscillation`: by how much the peak load of the smart plug will oscillate.
  - `sg.generator.on.probability`: the probability of the smart plug being on.
  - `sg.generator.on.lengths`: a comma-separated list of lengths of time to be selected from to set the amount of time that the smart plug will be on.

#### <a id="configuration-kafka-spout"></a>KafkaSpout

```
<app-prefix>.kafka.zookeeper.host=
<app-prefix>.kafka.spout.topic=
<app-prefix>.kafka.zookeeper.path=
<app-prefix>.kafka.consumer.id=
```

#### <a id="configuration-redis-spout"></a>RedisSpout

```
<app-prefix>.redis.server.host=
<app-prefix>.redis.server.port=
<app-prefix>.redis.server.pattern=
<app-prefix>.redis.server.queue_size=
```

#### <a id="configuration-twitter-streaming-spout"></a>TwitterStreamingSpout

```
<app-prefix>.twitter.consumer_key=
<app-prefix>.twitter.consumer_secret=
<app-prefix>.twitter.access_token=
<app-prefix>.twitter.access_token_secret=
```

### <a id="configuration-sinks"></a>Sinks

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

#### <a id="configuration-cassandra-batch-sink"></a>CassandraBatchSink

```
<app-prefix>.cassandra.host=
<app-prefix>.cassandra.keyspace=
<app-prefix>.cassandra.sink.column_family=
<app-prefix>.cassandra.sink.field.row_key=
<app-prefix>.cassandra.sink.ack_strategy=
```

#### <a id="configuration-count-batch-sink"></a>CassandraCountBatchSink

```
<app-prefix>.cassandra.host=
<app-prefix>.cassandra.keyspace=
<app-prefix>.cassandra.sink.column_family=
<app-prefix>.cassandra.sink.field.increment=
<app-prefix>.cassandra.sink.ack_strategy=
```

#### <a id="configuration-redis-sink"></a>RedisSink

```
<app-prefix>.redis.server.host=
<app-prefix>.redis.server.port=
<app-prefix>.redis.sink.queue=
```

#### <a id="configuration-socket-sink"></a>SocketSink

```
<app-prefix>.sink.socket.port=
<app-prefix>.sink.socket.charset=
```

## <a id="metrics"></a>Metrics

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
[23]: data/smart-grid.csv
[24]: https://drive.google.com/file/d/0B0TBL8JNn3JgV29HZWhSSVREQ0E/edit?usp=sharing
[25]: http://corsi.dei.polimi.it/distsys/2013-2014/projects.html
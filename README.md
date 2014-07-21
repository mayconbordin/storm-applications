storm-applications
==================

A collection of real-time applications built with Apache Storm.

## Usage

### Build

```bash
git clone git@github.com:mayconbordin/storm-applications.git
cd storm-applications/
mvn -P<profile> package
```

Use the `local` profile to run the applications in local mode or `cluster` to run in a remote cluster.

### Run Locally

```bash
java -jar target/storm-applications-*-with-dependencies.jar --app <application-name>
```

### Command Line Options

 - `-a, --app`: the application to be executed (required)
 - `-c, --config`: path to a *custom* configuration file
 - `-m, --mode`: `local` [default] to run locally or `remote` to run in a cluster
 - `-r, --runtime`: runtime in seconds for the application (local mode only) [default=300]
 - `-t, --topology-name`: the name of the topology (remote mode only)

## Configuration

Instead of each application having its own spouts and sinks (bolts that send data to other systems), we have defined a few basic spouts (`FileSpout`, `GeneratorSpout`, `KafkaSpout`, `RedisSpout`) and sinks (`FileSink`, `RedisSink`, `SocketSink`, `Cassandra...`, `NullSink`).

All but the `GeneratorSpout` need a `Parser`. The parser receives a string and returns a list of values, following the schema defined in the topology. To set a spout that reads from a file and parses the data as a Common Log Format, the configuration file would look like this:

```
<app-prefix>.spout.threads=1
<app-prefix>.spout.class=storm.applications.spout.FileSpout
<app-prefix>.spout.path=./data/http-server.log
<app-prefix>.spout.parser=storm.applications.spout.parser.CommonLogParser
```

The `GeneratorSpout` doesn't need a parser, instead it uses an instance of a class that extends the `Generator` class. Each time the generator is called it returns a new tuple.

Similarly, some sink classes (`FileSink`, `ConsoleSink`, `RedisSink`, `SocketSink`) need a `Formatter` which receives a tuple and returns a string. Writing the output of an application to a file could be achieved with the following configuration:

```
<app-prefix>.sink.threads=1
<app-prefix>.sink.class=storm.applications.sink.FileSink
<app-prefix>.sink.path=./output/result_%(taskid).dat
<app-prefix>.sink.formatter=storm.applications.sink.formatter.FullInfoFormatter
```

The `<app-prefix>` is the prefix of the application being executed, and the placeholder `%(taskid)` is used in the file path so that each instance of the `FileSink` can write to its own file, avoiding problems in case of two instances residing in the same machine.

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

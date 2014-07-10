storm-applications
==================

A collection of real-time applications built with Apache Storm.

## Usage

### Build

```bash
git clone git@github.com:mayconbordin/storm-applications.git
cd storm-applications/
mvn -Plocal package
```

Use the `local` profile to run the applications in local mode or `cluster` to run in a remote cluster.

### Run

```bash
java -jar target/storm-applications-*-with-dependencies.jar --app machine-outlier
```


## Applications

### Wordcount (WC)

The classic example of big data applications, the wordcount application was extracted from [storm-stater](https://github.com/nathanmarz/storm-starter).
It is composed of a bolt for splitting sentences into words and another one for counting the number of occurrences for each word in a hashmap.

### Trending Topics (TT)

Also taken from the [storm-stater](https://github.com/nathanmarz/storm-starter), it extracts the hashtags from tweets and keeps track of the number of occurrences in a rolling counter.
The full count is emitted periodically, ranked by number of occurrences and the ones with the highest counts are emitted in the end, as the trending topics.

### Bargain Index (BI)

This applications was taken from papers about the System S (IBM InfoSphere Streams).
First, the VWAP (Volume Weighted Average Price) is calculated from a stream of trades, 
then another bolt receives both the VWAP and another stream of quotes and calculates a 
bargain index that tells if it is a good idea to buy the quote that is being offered and how good it is.

### Fraud Detection in Credit Card Transactions (FD)

### Outlier Detection in Computer Network (MO)

### Spike Detection in Sensor Network (SD)

Tracks measurements from a set of sensor devices, calculates the moving average of
these measurements and checks if the current readings are above a certain threshold
in relation to the moving average, if so, an alert is emitted.

### Sentiment Analysis for Twitter (SA)

Calculates the sentiment score for each tweet and produces a summary per state. Uses
a very basic algorithm that counts occurrences of good and bad words in the message to
calculate the score.

### VoIPSTREAM (Spam Detection in VoIP) (VS)

VoIPSTREAM is an application composed of a set of filters and modules that are used
to detect telemarketing spam in Call Detail Records (CDRs). A detailed description
of the application can be found in the [paper](http://www.sigcomm.org/sites/default/files/ccr/papers/2011/October/2043165-2043167.pdf)
that describes an on-demand time-decaying bloom filter.

### Ads Analytics

Calculate the current Click-Through Rate (CTR) for pairs of query and ad. Predicts
the probability of a given ad being clicked given a set of features, such as the query,
position of the ad, the advertiser, etc.

### Reinforcement Learner (RL)

Reinforcement learning in the context of ads can be employed as a way of maximizing
the CTR by choosing the ad or ads with highest profit. As the time goes by, an ad
may be replaced by other ads as a response to a decreasing CTR.

### Spam Filter for Emails (SF)

### Log Processing (LP)

### Click Analytics (CA)

## Datasets

| Application        | Source           | Size  |
| ------------- |:-------------:| -----:|
| WC      | [Project Gutenberg](http://www.google.com/url?q=http%3A%2F%2Fwww.gutenberg.org%2F&sa=D&sntz=1&usg=AFQjCNFDqpf6tPxmqBeTFBhVOMwFjCXD7w) | ~8GB |
| BI      | [Yahoo Finance](https://finance.yahoo.com/), [Google Finance](https://www.google.com/finance)      |   — |
| SD | [Intel Berkeley Research Lab](http://db.csail.mit.edu/labdata/labdata.html)       |    150MB |
| TT, SA | [Twitter Streaming](https://dev.twitter.com/docs/api/streaming) | — |
| MO | [Google Cluster Traces](http://code.google.com/p/googleclusterdata/) | 36GB (compressed)
| CA, LP | [1998 World Cup Web Site Http Logs](http://ita.ee.lbl.gov/html/contrib/WorldCup.html) | 104GB |
| SF | <p>[TREC 2007 Public Spam Corpus](http://plg.uwaterloo.ca/~gvcormac/spam/)</P><p>[SPAM Archive by Bruce Guenter](http://untroubled.org/spam/)</p><p>[Enron Email Dataset](http://www.cs.cmu.edu/~./enron/)</p><p>[Enron Spam Dataset](http://nlp.cs.aueb.gr/software_and_datasets/Enron-Spam/index.html)</p> | <p>547MB (labeled)</p><p>~1.2GB (spam only)</p><p>2.6GB (raw)</p><p>50MB (labeled)</p> |

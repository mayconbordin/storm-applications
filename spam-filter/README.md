Spam Filter
===========


## Training

1. Receive a training message (spam or ham)
2. Split message into tokens
3. Emit each token with the number of ocurrences of it and the total number of occurrences
   for all tokens, so that the total count (for spam or ham) can be updated.
4. Tokens are distributed by the token name (hash modulus)
5. Each instance of the filter will train with a portion of the tokens.

## Analysis

Split message into tokens, each token goes to the filter that has the information
about the token, one component at the end will summarize the information and calculate
the probability of the message being a spam. Each message can have an ID, so that
multiple instances of the summarizor operator can exist, all the results for one message
ID will end up in the same summarizor instance.


## Build

https://github.com/wurstmeister/storm-kafka-0.8-plus-test
https://github.com/ashrithr/LogEventsProcessing


## Running

1) Start Zookeeper and Kafka

```bash
bin/start_server_processes.sh
```

2) Start the producer

```bash
java -cp spam-filter.jar org.storm.applications.producer.SpamFilterProducer producer.properties
```
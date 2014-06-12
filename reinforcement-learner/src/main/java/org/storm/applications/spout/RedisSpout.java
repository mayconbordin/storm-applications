/*
 * avenir: Predictive analytic based on Hadoop Map Reduce
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package org.storm.applications.spout;

import backtype.storm.spout.SpoutOutputCollector;
import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.storm.applications.util.ConfigUtility;

public class RedisSpout extends BaseRichSpout {
    private static final long serialVersionUID = 4571831489023437625L;
    private static final Logger LOG = Logger.getLogger(RedisSpout.class);
    private static final String NIL = "nil";
    
    private Jedis jedis;
    private String queue;
    private Fields fields;
    private SpoutOutputCollector collector;

    public RedisSpout(String queue, Fields fields) {
        this.queue = queue;
        this.fields = fields;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        
        String redisHost = ConfigUtility.getString(conf, "redis.server.host");
        int redisPort = ConfigUtility.getInt(conf, "redis.server.port");
        jedis = new Jedis(redisHost, redisPort);
    }

    public void nextTuple() {
        String message = jedis.rpop(queue);
        
        if (message != null && !message.equals(NIL)) {
            String[] items = message.split(",");
            collector.emit(new Values(items[0], Integer.parseInt(items[1])));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }
}

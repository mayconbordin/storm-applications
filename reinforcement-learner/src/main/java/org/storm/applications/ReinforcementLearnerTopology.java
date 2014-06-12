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

package org.storm.applications;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import static org.storm.applications.ReinforcementLearnerConstants.*;
import org.storm.applications.sink.RedisActionSink;
import org.storm.applications.bolt.ReinforcementLearnerBolt;
import org.storm.applications.spout.RedisSpout;
import org.storm.applications.topology.AbstractTopology;
import org.storm.applications.util.ConfigUtility;

/**
 * Builds and submits storm topology for reinforcement learning
 * @author pranab
 *
 */
public class ReinforcementLearnerTopology extends AbstractTopology {
    private String eventQueue;
    private String rewardQueue;
    private String actionQueue;
    private int spoutThreads;
    private int boltThreads;
    private int numWorkers;
    private int maxSpoutPending;
    private int maxTaskParalleism;
    
    public ReinforcementLearnerTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void prepare() {
        eventQueue  = ConfigUtility.getString(config, "redis.event.queue");
        rewardQueue = ConfigUtility.getString(config, "redis.reward.queue");
        actionQueue = ConfigUtility.getString(config, "redis.action.queue");
        
        spoutThreads = ConfigUtility.getInt(config, "spout.threads", 1);
        boltThreads  = ConfigUtility.getInt(config, "bolt.threads", 1);
        numWorkers   = ConfigUtility.getInt(config, "num.workers", 1);
        
        maxSpoutPending   = ConfigUtility.getInt(config, "max.spout.pending", 1000);
        maxTaskParalleism = ConfigUtility.getInt(config, "max.task.parallelism", 100);
    }

    public StormTopology buildTopology() {
        builder = new TopologyBuilder();

        RedisSpout eventSpout = new RedisSpout(eventQueue, new Fields(EVENT_ID, ROUND_NUM));
        builder.setSpout(EVENT_SPOUT, eventSpout, spoutThreads);
        
        RedisSpout rewardSpout = new RedisSpout(rewardQueue, new Fields(ACTION_ID, REWARD));
        builder.setSpout(REWARD_SPOUT, rewardSpout, spoutThreads);

        builder.setBolt(REINFORCE_BOLT, new ReinforcementLearnerBolt(), boltThreads)
               .shuffleGrouping(EVENT_SPOUT)
               .allGrouping(REWARD_SPOUT);
        
        builder.setBolt(ACTION_BOLT, new RedisActionSink(actionQueue))
               .shuffleGrouping(REINFORCE_BOLT);

        config.setNumWorkers(numWorkers);
        config.setMaxSpoutPending(maxSpoutPending);
        config.setMaxTaskParallelism(maxTaskParalleism);
        
        return builder.createTopology();
    }
}

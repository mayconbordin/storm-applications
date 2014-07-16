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

package storm.applications.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.ReinforcementLearnerConstants.*;
import storm.applications.bolt.ReinforcementLearnerBolt;
import storm.applications.sink.BaseSink;
import storm.applications.spout.AbstractSpout;

/**
 * Builds and submits storm topology for reinforcement learning
 * @author pranab
 *
 */
public class ReinforcementLearnerTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ReinforcementLearnerTopology.class);
    
    private AbstractSpout eventSpout;
    private AbstractSpout rewardSpout;
    private BaseSink actionSink;
    
    private int eventSpoutThreads;
    private int rewardSpoutThreads;
    private int learnerThreads;
    private int sinkThreads;
    
    public ReinforcementLearnerTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        eventSpout  = loadSpout("event");
        rewardSpout = loadSpout("reward");
        actionSink  = loadSink();
        
        eventSpoutThreads  = config.getInt(getConfigKey(Conf.SPOUT_THREADS, "event"), 1);
        rewardSpoutThreads = config.getInt(getConfigKey(Conf.SPOUT_THREADS, "reward"), 1);
        learnerThreads     = config.getInt(Conf.LEARNER_THREADS, 1);
        sinkThreads        = config.getInt(getConfigKey(Conf.SINK_THREADS), 1);
    }

    @Override
    public StormTopology buildTopology() {
        eventSpout.setFields(new Fields(Field.EVENT_ID, Field.ROUND_NUM));
        rewardSpout.setFields(new Fields(Field.ACTION_ID, Field.REWARD));
        
        builder.setSpout(Component.EVENT_SPOUT, eventSpout, eventSpoutThreads);
        builder.setSpout(Component.REWARD_SPOUT, rewardSpout, rewardSpoutThreads);

        builder.setBolt(Component.LEARNER, new ReinforcementLearnerBolt(), learnerThreads)
               .shuffleGrouping(Component.EVENT_SPOUT)
               .allGrouping(Component.REWARD_SPOUT);
        
        builder.setBolt(Component.SINK, actionSink, sinkThreads)
               .shuffleGrouping(Component.LEARNER);

        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}

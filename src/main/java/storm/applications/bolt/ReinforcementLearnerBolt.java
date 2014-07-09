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

package storm.applications.bolt;

import backtype.storm.task.OutputCollector;
import java.util.Map;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.ReinforcementLearnerConstants.*;
import storm.applications.model.learner.ReinforcementLearner;
import storm.applications.model.learner.ReinforcementLearnerFactory;
import storm.applications.util.ConfigUtility;

/**
 * Reinforcement learner bolt. Any RL algorithm can be used
 * @author pranab
 *
 */
public class ReinforcementLearnerBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ReinforcementLearnerBolt.class);
    private static final long serialVersionUID = 6746219511729480056L;

    private ReinforcementLearner learner = null;
    private OutputCollector collector;

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
        // intialize learner
        String learnerType = ConfigUtility.getString(stormConf, "reinforcement.learrner.type");
        String[] actions = ConfigUtility.getString(stormConf, "reinforcement.learrner.actions").split(",");
        Map<String, Object> typedConf = ConfigUtility.toTypedMap(stormConf);
        learner =  ReinforcementLearnerFactory.create(learnerType, actions, typedConf);
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals(EVENT_SPOUT)) {
            // select action for next round
            String eventID = input.getStringByField(EVENT_ID);
            int roundNum   = input.getIntegerByField(ROUND_NUM);
            
            String[] actions = learner.nextActions(roundNum);
            collector.emit(new Values(eventID, actions));
        }
        
        else if (input.getSourceComponent().equals(REWARD_SPOUT)) {
            // reward feedback
            String action = input.getStringByField(ACTION_ID);
            int reward    = input.getIntegerByField(REWARD);
            
            learner.setReward(action, reward);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_ID, ACTIONS));
    }
}

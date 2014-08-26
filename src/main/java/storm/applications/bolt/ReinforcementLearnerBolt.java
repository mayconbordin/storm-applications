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

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.ReinforcementLearnerConstants.*;
import storm.applications.model.learner.ReinforcementLearner;
import storm.applications.model.learner.ReinforcementLearnerFactory;

/**
 * Reinforcement learner bolt. Any RL algorithm can be used
 * @author pranab
 *
 */
public class ReinforcementLearnerBolt extends AbstractBolt {
    private ReinforcementLearner learner;

    @Override
    public void initialize() {
        String learnerType = config.getString(Conf.LEARNER_TYPE);
        String[] actions   = config.getString(Conf.LEARNER_ACTIONS).split(",");
        
        learner =  ReinforcementLearnerFactory.create(learnerType, actions, config);
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals(Component.EVENT_SPOUT)) {
            // select action for next round
            String eventID = input.getStringByField(Field.EVENT_ID);
            int roundNum   = input.getIntegerByField(Field.ROUND_NUM);
            
            String[] actions = learner.nextActions(roundNum);
            collector.emit(input, new Values(eventID, actions));
        }
        
        else if (input.getSourceComponent().equals(Component.REWARD_SPOUT)) {
            // reward feedback
            String action = input.getStringByField(Field.ACTION_ID);
            int reward    = input.getIntegerByField(Field.REWARD);
            
            learner.setReward(action, reward);
        }
        
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.EVENT_ID, Field.ACTIONS);
    }
}

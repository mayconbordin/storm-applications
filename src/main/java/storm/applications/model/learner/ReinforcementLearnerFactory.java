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
package storm.applications.model.learner;

import storm.applications.constants.ReinforcementLearnerConstants.Learner;
import storm.applications.util.config.Configuration;

/**
 * Factory to create reinforcement learner
 * @author pranab
 *
 */
public class ReinforcementLearnerFactory {
    /**
     * @param learnerID
     * @param actions
     * @param config
     * @return
     */
    public static ReinforcementLearner create(String learnerID, String[] actions, Configuration config) {
        ReinforcementLearner learner = null;
        
        switch (learnerID) {
            case Learner.INTERVAL_ESTIMATOR:
                learner = new IntervalEstimator();
                break;
            case Learner.SAMPSON_SAMPLER:
                learner = new SampsonSampler();
                break;
            case Learner.OPT_SAMPSON_SAMPLER:
                learner = new OptimisticSampsonSampler();
                break;
            case Learner.RANDOM_GREEDY:
                learner = new RandomGreedyLearner();
                break;
        }
        
        if (learner != null) {
            learner.withActions(actions).initialize(config);
        }
        
        return learner;
    }
}

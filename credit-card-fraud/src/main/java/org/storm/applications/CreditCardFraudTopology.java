/*
 * beymani: Outlier and anamoly detection 
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
import static org.storm.applications.CreditCardFraudConstants.*;
import org.storm.applications.bolt.PredictorBolt;
import org.storm.applications.spout.TransactionFileSpout;
import org.storm.applications.topology.AbstractTopology;
import org.storm.applications.util.ConfigUtility;

/**
 * Storm topolgy driver for outlier detection
 * @author pranab
 */
public class CreditCardFraudTopology extends AbstractTopology {
    private int spoutThreads;
    private int boltThreads;
    private String spoutPath;
    
    public CreditCardFraudTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    public void prepare() {
        spoutThreads = ConfigUtility.getInt(config, "predictor.spout.threads");
        boltThreads  = ConfigUtility.getInt(config, "predictor.bolt.threads");
        spoutPath = ConfigUtility.getString(config, "predictor.spout.path");
    }
    
    public StormTopology buildTopology() {
        builder = new TopologyBuilder();

        builder.setSpout(TRANSACTION_SPOUT, new TransactionFileSpout(spoutPath), spoutThreads);
        
        builder.setBolt(PREDICTOR_BOLT, new PredictorBolt(), boltThreads)
               .fieldsGrouping(TRANSACTION_SPOUT, new Fields(ENTITY_ID_FIELD));
        
        return builder.createTopology();
    }
}

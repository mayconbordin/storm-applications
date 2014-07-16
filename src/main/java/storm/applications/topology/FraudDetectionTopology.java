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

package storm.applications.topology;


import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.FraudDetectionConstants.*;
import storm.applications.bolt.FraudPredictorBolt;

/**
 * Storm topolgy driver for outlier detection
 * @author pranab
 */
public class FraudDetectionTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionTopology.class);
    
    private int predictorThreads;
    
    public FraudDetectionTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        predictorThreads  = config.getInt(Conf.PREDICTOR_THREADS, 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(Field.ENTITY_ID, Field.RECORD_DATA));
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.PREDICTOR, new FraudPredictorBolt(), predictorThreads)
               .fieldsGrouping(Component.SPOUT, new Fields(Field.ENTITY_ID));
        
        builder.setBolt(Component.SINK, sink, sinkThreads)
               .fieldsGrouping(Component.PREDICTOR, new Fields(Field.ENTITY_ID));
        
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

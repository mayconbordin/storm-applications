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

package storm.applications.spout;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static storm.applications.constants.FraudDetectionConstants.*;

/**
 * @author pranab
 *
 */
public class TransactionFileSpout extends AbstractFileSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionFileSpout.class);

    public TransactionFileSpout(String path) {
        super(path);
    }

    @Override
    protected Values nextRecord(String strRecord) {
        String[] items = strRecord.split(",", 2);
        return new Values(items[0], items[1]);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ENTITY_ID_FIELD, RECORD_DATA_FIELD));		
    }
}	

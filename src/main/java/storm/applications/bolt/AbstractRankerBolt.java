/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.applications.bolt;

import backtype.storm.Config;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import storm.applications.constants.TrendingTopicsConstants.Field;
import storm.applications.tools.Rankings;
import storm.applications.util.stream.TupleUtils;

/**
 * This abstract bolt provides the basic behavior of bolts that rank objects according to their count.
 * <p/>
 * It uses a template method design pattern for {@link AbstractRankerBolt#execute(Tuple, BasicOutputCollector)} to allow
 * actual bolt implementations to specify how incoming tuples are processed, i.e. how the objects embedded within those
 * tuples are retrieved and counted.
 */
public abstract class AbstractRankerBolt extends AbstractBolt {
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private final int emitFrequencyInSeconds;
    private final int count;
    private final Rankings rankings;

    public AbstractRankerBolt() {
        this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public AbstractRankerBolt(int topN) {
        this(topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public AbstractRankerBolt(int topN, int emitFrequencyInSeconds) {
        if (topN < 1) {
          throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }
        if (emitFrequencyInSeconds < 1) {
          throw new IllegalArgumentException(
              "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }
        count = topN;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        rankings = new Rankings(count);
    }
    
    @Override
    public void initialize() {
        
    }

    protected Rankings getRankings() {
        return rankings;
    }

    @Override
    public final void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            getLogger().info("Received tick tuple, triggering emit of current rankings");
            emitRankings();
        }
        else {
            updateRankingsWithTuple(tuple);
            collector.ack(tuple);
        }
    }

    abstract void updateRankingsWithTuple(Tuple tuple);

    private void emitRankings() {
        collector.emit(new Values(rankings.copy()));
        getLogger().debug("Rankings: " + rankings);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.RANKINGS);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    abstract Logger getLogger();
}
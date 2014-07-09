package storm.applications.spout;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TwitterFileStreamSpout extends AbstractFileSpout {
    private static Logger LOG = LoggerFactory.getLogger(TwitterFileStreamSpout.class);
    private static final JSONParser jsonParser = new JSONParser();

    public TwitterFileStreamSpout(String sourceFile) {
        super(sourceFile);
    }
    
    protected Values nextRecord(String strRecord) {
        try {
            Object json = jsonParser.parse(strRecord);
            return new Values(json);
        } catch (ParseException e) {
            LOG.error("Error parsing JSON encoded tweet", e);
        }
        
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }
}
package storm.applications.sink;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import storm.applications.util.StringUtil;

public class FileSink extends BaseSink {
    private static Logger LOG = Logger.getLogger(FileSink.class);
            
    private Writer writer = null;
    private String file;
    
    public FileSink(String file) {
        this.file = file;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Map<String, Object> map = new HashMap<String, Object>(3);
        map.put("taskid", context.getThisTaskId());
        map.put("taskindex", context.getThisTaskIndex());
        map.put("componentid", context.getThisComponentId());
        
        file = StringUtil.dictFormat(file, map);

        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                  new FileOutputStream(file), "utf-8"));
        } catch (IOException ex) {
            LOG.error(ex);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            writer.write(formatTuple(tuple) + "\n");
        } catch (IOException ex) {
            LOG.error(ex);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        try {
            writer.close();
        } catch (IOException ex) {
            LOG.error(ex);
        }
    }
    
    protected String formatTuple(Tuple tuple) {
        String line = "";
            
        for (int i=0; i<tuple.size(); i++) {
            if (i != 0) line += ";";
            line += String.format("%s", tuple.getValue(i));
        }
        
        return line;
    }
    
}

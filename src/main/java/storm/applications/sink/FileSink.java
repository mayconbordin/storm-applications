package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.util.data.StringUtil;

public class FileSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(FileSink.class);
            
    protected BufferedWriter writer = null;
    protected String file;

    @Override
    public void initialize() {
        super.initialize();
        
        file = config.getString(getConfigKey(BaseConf.SINK_PATH));
        
        Map<String, Object> map = new HashMap<>(3);
        map.put("taskid", context.getThisTaskId());
        map.put("taskindex", context.getThisTaskIndex());
        map.put("componentid", context.getThisComponentId());
        
        file = StringUtil.dictFormat(file, map);

        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                  new FileOutputStream(file), "utf-8"));
        } catch (IOException ex) {
            LOG.error("Error while creating file " + file, ex);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            writer.write(formatter.format(tuple));
            collector.ack(tuple);
        } catch (IOException ex) {
            LOG.error("Error while writing to file " + file, ex);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        
        try {
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            LOG.error("Error while closing the file " + file, ex);
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}

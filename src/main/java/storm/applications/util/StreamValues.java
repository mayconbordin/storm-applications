package storm.applications.util;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StreamValues extends Values {
    private String streamId = Utils.DEFAULT_STREAM_ID;
    
    public StreamValues() {
    }

    public StreamValues(Object... vals) {
        super(vals);
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }
    
}

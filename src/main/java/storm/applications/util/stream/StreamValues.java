package storm.applications.util.stream;

import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StreamValues extends Values {
    private Object messageId;
    private String streamId = Utils.DEFAULT_STREAM_ID;
    
    public StreamValues() {
    }

    public StreamValues(Object... vals) {
        super(vals);
    }

    public Object getMessageId() {
        return messageId;
    }

    public void setMessageId(Object messageId) {
        this.messageId = messageId;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }
    
}

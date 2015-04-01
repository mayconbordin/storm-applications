package storm.applications.util.stream;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

public class TupleUtils {
    /**
     * @param tuple
     * @return true if this is "tick" tuple emitted by the Storm framework
     */
    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
        Constants.SYSTEM_TICK_STREAM_ID);
    }
}
package storm.applications.model.fraud.prepare;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import storm.applications.util.data.Tuple;

public class StateTransitionCombiner extends Reducer<Tuple, IntWritable, Tuple, IntWritable> {
    private int count;
    private IntWritable outVal = new IntWritable();

    @Override
    protected void reduce(Tuple  key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        outVal.set(count);
        context.write(key, outVal);       	
    }
}	
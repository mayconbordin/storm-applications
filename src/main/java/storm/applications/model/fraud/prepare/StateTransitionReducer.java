package storm.applications.model.fraud.prepare;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import storm.applications.util.math.StateTransitionProbability;
import storm.applications.util.data.Tuple;

public class StateTransitionReducer extends Reducer<Tuple, IntWritable, NullWritable, Text> {
    private String fieldDelim;
    private Text outVal  = new Text();
    private String[] states;
    private StateTransitionProbability transProb;
    private int count;

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelim = conf.get("field.delim.out", ",");
        states = conf.get("model.states").split(",");
        transProb = new StateTransitionProbability(states, states);
        int transProbScale = conf.getInt("trans.prob.scale", 1000);
        transProb.setScale(transProbScale);
    }

    @Override
    protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        //all states
        Configuration conf = context.getConfiguration();
        outVal.set(conf.get("model.states"));
        context.write(NullWritable.get(),outVal);

        //state transitions
        transProb.normalizeRows();
        for (int i = 0; i < states.length; ++i) {
            String val = transProb.serializeRow(i);
            outVal.set(val);
            context.write(NullWritable.get(), outVal);
        }
    }

    @Override
    protected void reduce(Tuple  key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        count = 0;
        for (IntWritable value : values) {
                count += value.get();
        }
        String fromSt = key.getString(0);
        String toSt = key.getString(1);
        transProb.add(fromSt, toSt, count);
    }	   	
}
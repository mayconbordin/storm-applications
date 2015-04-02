package storm.applications.model.fraud.prepare;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import storm.applications.util.data.Tuple;

public class StateTransitionMapper extends Mapper<LongWritable, Text, Tuple, IntWritable> {
    private String fieldDelimRegex;
    private String[] items;
    private int skipFieldCount;
    private Tuple outKey = new Tuple();
    private IntWritable outVal  = new IntWritable(1);

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fieldDelimRegex = conf.get("field.delim.regex", ",");
        skipFieldCount = conf.getInt("skip.field.count", 0);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        items  =  value.toString().split(fieldDelimRegex);
        if (items.length >= (skipFieldCount + 2)) {
            for (int i = skipFieldCount + 1; i < items.length; ++i) {
                outKey.initialize();
                outKey.add(items[i-1], items[i]);
                context.write(outKey, outVal);
            }
        }
    }
}	
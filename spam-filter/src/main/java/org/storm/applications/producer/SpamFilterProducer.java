package org.storm.applications.producer;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.storm.applications.SpamFilterConstants.Field;
import org.storm.applications.util.PropertiesUtils;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SpamFilterProducer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SpamFilterProducer.class);
    public static final String TRAINING_PRODUCER = "training";
    public static final String ANALYSIS_PRODUCER = "analysis";
            
    private String type;
    private String topic;
    private Properties properties;
    private Producer<String, String> producer;

    public SpamFilterProducer(String type, Properties properties, Producer<String, String> producer) {
        this.type = type;
        this.properties = properties;
        this.producer = producer;
        
        topic = properties.getProperty("producer." + type + ".topic");
    }
    
    public static void main(String args[]) throws InterruptedException {
        Properties props = PropertiesUtils.fromFile(args[0]);
        ProducerConfig config = new ProducerConfig(props);        
        
        Thread trainingThread = new Thread(new SpamFilterProducer(TRAINING_PRODUCER, props, new Producer<String, String>(config)));
        Thread analysisThread = new Thread(new SpamFilterProducer(ANALYSIS_PRODUCER, props, new Producer<String, String>(config)));

        trainingThread.start();
        analysisThread.start();
    }

    @Override
    public void run() {
        String dir = properties.getProperty("producer." + type + ".dir");

        try {
            File index = new File(properties.getProperty("producer." + type + ".index"));
            List<String> files = Files.readLines(index, Charset.defaultCharset());
        
            for (int i=0; i<files.size(); i++) {
                if (i%1000 == 0)
                    LOG.info(String.format("[%s] Read %d of %d emails", type, (i+1), files.size()));

                String[] file = files.get(i).split("\t");
                boolean isSpam = file[0].toLowerCase().trim().equals("spam");
                String content = Files.toString(new File(dir + "/" + file[1]), Charset.defaultCharset());
                
                JSONObject obj = new JSONObject();
                obj.put(Field.ID, file[1]);
                obj.put(Field.MESSAGE, content);
                obj.put(Field.IS_SPAM, isSpam);
                
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, file[1], obj.toJSONString());
                producer.send(data);
            }
            
            LOG.info(String.format("[%s] Finished reading %d emails", type, files.size()));
        } catch (IOException ex) {
            LOG.error(String.format("[%s] Unable to read email message", type), ex);
        }
    }
}

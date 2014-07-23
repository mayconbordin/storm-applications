package storm.applications.spout.generator;

import java.util.Random;
import storm.applications.util.Configuration;
import storm.applications.util.StreamValues;

public class RandomSentenceGenerator extends Generator {
    private static final String[] sentences = new String[]{
        "the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs",
        "i am at two with nature"
    };
    
    private Random rand;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        rand = new Random();
    }
    
    @Override
    public StreamValues generate() {
        return new StreamValues(sentences[rand.nextInt(sentences.length)]);
    }
    
}

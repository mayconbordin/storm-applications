package storm.applications.spout.generator;

import java.util.Random;
import org.joda.time.DateTime;
import storm.applications.constants.VoIPSTREAMConstants.Conf;
import storm.applications.model.cdr.CDRDataGenerator;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.config.Configuration;
import storm.applications.util.math.RandomUtil;
import storm.applications.util.stream.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CDRGenerator extends Generator {
    private String[] phoneNumbers;
    private int population;
    private double errorProb;
    private Random rand = new Random();

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        population = config.getInt(Conf.GENERATOR_POPULATION, 50);
        errorProb  = config.getDouble(Conf.GENERATOR_POPULATION, 0.05);
        
        phoneNumbers = new String[population];
        
        for (int i=0; i<population; i++) {
            phoneNumbers[i] = CDRDataGenerator.phoneNumber("US", 11);
        }
    }
    
    @Override
    public StreamValues generate() {
        CallDetailRecord cdr = new CallDetailRecord();
        
        cdr.setCallingNumber(pickNumber());
        cdr.setCalledNumber(pickNumber(cdr.getCallingNumber()));
        cdr.setAnswerTime(DateTime.now().plusMinutes(RandomUtil.randInt(0, 60)));
        cdr.setCallDuration(RandomUtil.randInt(0, 60 * 5));
        cdr.setCallEstablished(CDRDataGenerator.causeForTermination(errorProb) == CDRDataGenerator.TERMINATION_CAUSE_OK);
        
        return new StreamValues(cdr.getCallingNumber(), cdr.getCalledNumber(), cdr.getAnswerTime(), cdr);
    }
    
    private String pickNumber(String excluded) {
        String number = "";
        while (number.isEmpty() || number.equals(excluded)) {
            number = phoneNumbers[rand.nextInt(population)];
        }
        return number;
    }
    
    private String pickNumber() {
        return phoneNumbers[rand.nextInt(population)];
    }
}

package org.storm.applications.spout;

import org.storm.applications.cdr.CDRDataGenerator;
import org.storm.applications.cdr.CallDetailRecord;
import org.storm.applications.util.RandomUtil;
import java.util.Random;
import org.joda.time.DateTime;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CDRGeneratorSpout extends CDRBaseSpout {
    private String[] phoneNumbers;
    private int population;
    private Random rand = new Random();

    public CDRGeneratorSpout() {
        population = 50;
        phoneNumbers = new String[population];
        
        for (int i=0; i<population; i++) {
            phoneNumbers[i] = CDRDataGenerator.phoneNumber();
        }
    }

    @Override
    public CallDetailRecord nextRecord() {
        CallDetailRecord cdr = new CallDetailRecord();
        
        cdr.setCallingNumber(pickNumber());
        cdr.setCalledNumber(pickNumber(cdr.getCallingNumber()));
        cdr.setAnswerTime(DateTime.now().plusMinutes(RandomUtil.randInt(0, 60)));
        //cdr.setCallDuration(RandomUtil.randInt(0, 3600 * 24));
        cdr.setCallDuration(RandomUtil.randInt(0, 60 * 5));
        cdr.setCallEstablished(CDRDataGenerator.causeForTermination(0.05) == CDRDataGenerator.TERMINATION_CAUSE_OK);
        return cdr;
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

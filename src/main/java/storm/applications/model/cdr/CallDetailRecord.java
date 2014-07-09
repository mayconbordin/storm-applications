package storm.applications.model.cdr;

import java.io.Serializable;
import org.joda.time.DateTime;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CallDetailRecord implements Serializable {
    private String callingNumber;
    private String calledNumber;
    private DateTime answerTime;
    private int callDuration;
    private boolean callEstablished;

    public String getCallingNumber() {
        return callingNumber;
    }

    public void setCallingNumber(String callingNumber) {
        this.callingNumber = callingNumber;
    }

    public String getCalledNumber() {
        return calledNumber;
    }

    public void setCalledNumber(String calledNumber) {
        this.calledNumber = calledNumber;
    }

    public DateTime getAnswerTime() {
        return answerTime;
    }

    public void setAnswerTime(DateTime answerTime) {
        this.answerTime = answerTime;
    }

    public int getCallDuration() {
        return callDuration;
    }

    public void setCallDuration(int callDuration) {
        this.callDuration = callDuration;
    }

    public boolean isCallEstablished() {
        return callEstablished;
    }

    public void setCallEstablished(boolean callEstablished) {
        this.callEstablished = callEstablished;
    }

    @Override
    public String toString() {
        return "CallDetailRecord{" + "callingNumber=" + callingNumber
                + ", calledNumber=" + calledNumber + ", answerTime=" + answerTime
                + ", callDuration=" + callDuration + ", callEstablished=" 
                + callEstablished + '}';
    }
}

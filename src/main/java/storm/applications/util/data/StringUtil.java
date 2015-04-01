package storm.applications.util.data;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

public class StringUtil {
    /**
     * Replace placeholders in a string by the values of the map structure.
     * @author: http://stackoverflow.com/a/2295004/794395
     * @param format The string to be formatted
     * @param values The key/value map for the replacement of the placeholders
     * @return the formatted string
     */
    public static String dictFormat(String format, Map<String, Object> values) {
        StringBuilder convFormat = new StringBuilder(format);
        ArrayList valueList = new ArrayList();
        int currentPos = 1;
        
        
        for (Entry<String, Object> entry : values.entrySet()) {
            String key = entry.getKey(),
              formatKey = "%(" + key + ")",
              formatPos = "%" + Integer.toString(currentPos) + "$s";
            
            int index = -1;
            
            while ((index = convFormat.indexOf(formatKey, index)) != -1) {
                convFormat.replace(index, index + formatKey.length(), formatPos);
                index += formatPos.length();
               
            }
             valueList.add(entry.getValue());
                currentPos++;
        }
        
        System.out.println(convFormat.toString());
        
        return String.format(convFormat.toString(), valueList.toArray());
    }
}

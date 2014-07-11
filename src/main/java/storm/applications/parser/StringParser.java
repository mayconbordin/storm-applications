package storm.applications.parser;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StringParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);

    @Override
    public StreamValues parse(String str) {
        if (StringUtils.isEmpty(str))
            return null;
        
        return new StreamValues(str);
    }
}
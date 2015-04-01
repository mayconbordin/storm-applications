package storm.applications.util.config;

import backtype.storm.Config;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtils {
    public static final Logger LOG = LoggerFactory.getLogger(PropertiesUtils.class);
    
    public static Properties fromFile(String configFilePath) {
        FileInputStream fis = null;
        
        try {
            fis = new FileInputStream(configFilePath);
            Properties configProps = new Properties();
            configProps.load(fis);
            return configProps;
        } catch (FileNotFoundException ex) {
            LOG.error("Config file not found", ex);
        } catch (IOException ex) {
            LOG.error("Error while reading config file", ex);
        } finally {
            try {
                if (fis != null)
                    fis.close();
            } catch (IOException ex) {
                LOG.warn("An error ocurred while closing the config file", ex);
            }
        }
        
        return null;
    }
}

package storm.applications.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants;
import storm.applications.generator.Generator;
import storm.applications.util.ClassLoaderUtils;
import storm.applications.util.ConfigUtility;
import storm.applications.util.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSpout.class);
    private Generator generator;

    @Override
    protected void initialize() {
        String generatorClass = getGeneratorClass();
        generator = (Generator) ClassLoaderUtils.newInstance(generatorClass, "parser", LOG);
        generator.initialize(config);
    }

    @Override
    public void nextTuple() {
        StreamValues values = generator.generate();
        collector.emit(values.getStreamId(), values);
    }
    
    protected String getGeneratorClass() {
        return ConfigUtility.getString(config, BaseConstants.BaseConf.SPOUT_GENERATOR);
    }
}

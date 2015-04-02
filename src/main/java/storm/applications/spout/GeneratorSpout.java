package storm.applications.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.spout.generator.Generator;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.stream.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSpout.class);
    private Generator generator;

    @Override
    protected void initialize() {
        String generatorClass = config.getString(getConfigKey(BaseConf.SPOUT_GENERATOR));
        generator = (Generator) ClassLoaderUtils.newInstance(generatorClass, "parser", LOG);
        generator.initialize(config);
    }

    @Override
    public void nextTuple() {
        StreamValues values = generator.generate();
        collector.emit(values.getStreamId(), values);
    }
}

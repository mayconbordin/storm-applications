package storm.applications.spout;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.parser.Parser;
import storm.applications.util.ClassLoaderUtils;
import storm.applications.util.ConfigUtility;
import storm.applications.util.StreamValues;

public abstract class FileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FileSpout.class);
    
    protected Parser parser;
    protected File[] files;
    protected Scanner scanner;
    protected int curFileIndex = 0;
    
    private String pathKey   = BaseConf.SPOUT_PATH;
    private String parserKey = BaseConf.SPOUT_PARSER;
    
    @Override
    public void initialize() {
        String parserClass = ConfigUtility.getString(config, parserKey);
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        buildIndex();
        openNextFile();
    }
    
    protected void buildIndex() {
        File dir = new File(ConfigUtility.getString(config, pathKey));
        
        if (dir.isDirectory()) {
            files = dir.listFiles();
        } else {
            files = new File[1];
            files[0] = dir;
        }
        
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                int res = f1.lastModified() < f2.lastModified() ? -1 : ( f1.lastModified() > f2.lastModified() ? 1 : 0);
                return res;
            }
        });
    }
    
    @Override
    public void nextTuple() {
        String value = readFile();
        
        if (value == null)
            return;
        
        StreamValues values = parser.parse(value);
        
        if (values != null) {
            collector.emit(values.getStreamId(), values);
        }
    }

    protected String readFile() {
        String record = null;
        if (scanner.hasNextLine()) {
            record = scanner.nextLine();
        } else {
            if (++curFileIndex < files.length) {
                openNextFile();
                if (scanner.hasNextLine()) {
                     record = scanner.nextLine();
                }				 
            } else {
                LOG.info("No more files to read");
            }
        }
        return record;
    }

    protected void openNextFile() {
        try {
            scanner = new Scanner(files[curFileIndex]);
        } catch (FileNotFoundException e) {
            LOG.error("File " + files[curFileIndex] + " was not found", e);
            throw new IllegalStateException("file not found");
        }
    }

    public void setPathKey(String pathKey) {
        this.pathKey = pathKey;
    }

    public void setParserKey(String parserKey) {
        this.parserKey = parserKey;
    }
}	

package org.storm.applications.spout;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Scanner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

public abstract class AbstractFileSpout extends BaseRichSpout {
    private static final Logger LOG = Logger.getLogger(AbstractFileSpout.class);
    
    protected SpoutOutputCollector collector;
    protected String path;
    protected File[] files;
    protected Scanner scanner;
    protected int curFileIndex = 0;

    /**
     * Create a new spout that will generate records by reading the file(s) in the path.
     * @param path The path to a file or directory
     */
    public AbstractFileSpout(String path) {
        this.path = path;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        File dir = new File(path);
        
        if (dir.isDirectory()) {
            files = dir.listFiles();
        } else {
            files = new File[1];
            files[0] = dir;
        }
        
        Arrays.sort(files, new Comparator<File>() {
            public int compare(File f1, File f2) {
                int res = f1.lastModified() < f2.lastModified() ? -1 : ( f1.lastModified() > f2.lastModified() ? 1 : 0);
                return res;
            }
        });

        openNextFile();
    }

    @Override
    public void nextTuple() {
        String strRecord = readFile();
        
        if (strRecord != null) {
            Values record = nextRecord(strRecord);

            if (record != null) {
                collector.emit(record);
            }
        } else {
            LOG.info("End of data");
        }
    }
    
    protected abstract Values nextRecord(String strRecord);

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
                //no more files to read
                LOG.info("No more files to read");
            }
        }
        return record;
    }

    protected void openNextFile() {
        try {
            scanner = new Scanner(files[curFileIndex]);
        } catch (FileNotFoundException e) {
            LOG.error(e);
            throw new IllegalStateException("file not found");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));		
    }
}	

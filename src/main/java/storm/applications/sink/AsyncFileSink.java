package storm.applications.sink;

import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class AsyncFileSink extends FileSink implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncFileSink.class);
    
    private BlockingQueue<String> queue;
    private ExecutorService executor;
    
    @Override
    public void initialize() {
        super.initialize();
        
        queue = new LinkedBlockingQueue<>();
        
        executor = Executors.newSingleThreadExecutor();
        executor.submit(this);
    }
    
    @Override
    public void execute(Tuple tuple) {
        try {
            queue.add(formatter.format(tuple));
            collector.ack(tuple);
        } catch (IllegalStateException ex) {
            LOG.warn("Queue is full", ex);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        executor.shutdown();
    }

    @Override
    public void run() {
        while (true) {
            try {
                String line = queue.take();
                writer.write(line);
                writer.newLine();
            } catch (InterruptedException ex) {
                LOG.error("Unable to get line from queue", ex);
            } catch (IOException ex) {
                LOG.error("Unable to write line to " + file, ex);
            }
        }
    }
}

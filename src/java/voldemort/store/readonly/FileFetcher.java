package voldemort.store.readonly;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.utils.EventThrottler;
import voldemort.utils.Props;
import voldemort.utils.Utils;

/**
 * An interface to fetch data for readonly store. The fetch could be via rsync
 * or hdfs. If the store is already on the local filesystem then no fetcher is
 * needed.
 * 
 * All implementations must provide a public constructor that takes
 * VoldemortConfig as a parameter.
 * 
 * 
 */
public abstract class FileFetcher {

    protected static final Logger logger = Logger.getLogger(FileFetcher.class);

    /**
     * Strings for parameters
     */
    protected final static String MAX_BYTES_PER_SECOND_STRING = "fetcher.max.bytes.per.sec";
    protected final static String REPORTING_INTERVAL_BYTES_STRING = "fetcher.reporting.interval.bytes";
    protected final static String FETCHER_BUFFER_SIZE_STRING = "fetcher.buffer.size";
    protected final static String HDFS_FETCHER_BUFFER_SIZE_STRING = "hdfs.fetcher.buffer.size";

    protected final static long REPORTING_INTERVAL_BYTES = 25 * 1024 * 1024;
    protected final static int DEFAULT_BUFFER_SIZE = 64 * 1024;

    protected final Long maxBytesPerSecond;
    protected final Long reportingIntervalBytes;
    protected final int bufferSize;
    protected static final AtomicInteger copyCount = new AtomicInteger(0);
    protected AsyncOperationStatus status;
    protected EventThrottler throttler = null;

    public FileFetcher(Props props) {
        this.maxBytesPerSecond = props.containsKey(MAX_BYTES_PER_SECOND_STRING) ? props.getBytes(MAX_BYTES_PER_SECOND_STRING)
                                                                               : null;
        if(this.maxBytesPerSecond != null)
            this.throttler = new EventThrottler(this.maxBytesPerSecond);
        this.reportingIntervalBytes = Utils.notNull(props.getBytes(REPORTING_INTERVAL_BYTES_STRING,
                                                                   REPORTING_INTERVAL_BYTES));

        // Keeping HDFS parameter for backwards compatibility
        this.bufferSize = (int) props.getBytes(FETCHER_BUFFER_SIZE_STRING,
                                               props.getBytes(HDFS_FETCHER_BUFFER_SIZE_STRING,
                                                              DEFAULT_BUFFER_SIZE));
        this.status = null;

    }

    public abstract File fetch(String source, String dest) throws IOException;

    public void setAsyncOperationStatus(AsyncOperationStatus status) {
        this.status = status;
    }
}

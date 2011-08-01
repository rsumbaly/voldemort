package voldemort.store.readonly.fetcher;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.utils.Time;

public class CopyStats {

    private final String fileName;
    private volatile long bytesSinceLastReport;
    private volatile long totalBytesCopied;
    private volatile long lastReportNs;
    private volatile long totalBytes;

    public CopyStats(String fileName, long totalBytes) {
        this.fileName = fileName;
        this.totalBytesCopied = 0L;
        this.bytesSinceLastReport = 0L;
        this.totalBytes = totalBytes;
        this.lastReportNs = System.nanoTime();
    }

    public void recordBytes(long bytes) {
        this.totalBytesCopied += bytes;
        this.bytesSinceLastReport += bytes;
    }

    public void reset() {
        this.bytesSinceLastReport = 0;
        this.lastReportNs = System.nanoTime();
    }

    public long getBytesSinceLastReport() {
        return bytesSinceLastReport;
    }

    public double getPercentCopied() {
        if(totalBytes == 0) {
            return 0.0;
        } else {
            return (double) (totalBytesCopied * 100) / (double) totalBytes;
        }
    }

    @JmxGetter(name = "totalBytesCopied", description = "The total number of bytes copied so far in this transfer.")
    public long getTotalBytesCopied() {
        return totalBytesCopied;
    }

    @JmxGetter(name = "bytesPerSecond", description = "The rate of the transfer in bytes/second.")
    public double getBytesPerSecond() {
        double ellapsedSecs = (System.nanoTime() - lastReportNs) / (double) Time.NS_PER_SECOND;
        return bytesSinceLastReport / ellapsedSecs;
    }

    @JmxGetter(name = "filename", description = "The file path being copied.")
    public String getFilename() {
        return this.fileName;
    }
}

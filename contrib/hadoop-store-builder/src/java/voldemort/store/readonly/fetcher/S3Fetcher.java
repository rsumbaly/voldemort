package voldemort.store.readonly.fetcher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.InputStreamMap;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;

import voldemort.store.readonly.FileFetcher;
import voldemort.utils.Props;
import voldemort.utils.Utils;

/**
 * A fetcher that gets files from S3
 * 
 */
public class S3Fetcher extends FileFetcher {

    private final String identity;
    private final String credential;

    public S3Fetcher(Props props) {
        super(props);
        this.identity = Utils.notNull(props.getString("fetcher.identity"));
        this.credential = Utils.notNull(props.getString("fetcher.credential"));

        logger.info("Created S3 fetcher with throttle rate " + maxBytesPerSecond
                    + ", reporting interval bytes " + reportingIntervalBytes);
    }

    @Override
    public File fetch(String sourceFileUrl, String destinationFile) throws IOException {
        BlobStoreContext context = null;
        try {
            context = new BlobStoreContextFactory().createContext("aws-s3", identity, credential);

            // Get reference to the store
            BlobStore store = context.getBlobStore();

            // Check if directory exists
            System.out.println(store.directoryExists("rsumbaly", "voldemort/node-0"));

            // Retrieve list of names and size
            for(StorageMetadata metadata: store.list("rsumbaly",
                                                     ListContainerOptions.Builder.inDirectory("voldemort/node-0"))) {
                System.out.println("NAME - " + metadata.getName() + " - "
                                   + metadata.getUserMetadata() + " - " + metadata.getETag()
                                   + " - " + metadata.getProviderId() + " - " + metadata.getType()
                                   + " - " + metadata.getUri() + " -  " + metadata.getLocation());
            }

            InputStreamMap map = context.createInputStreamMap("rsumbaly",
                                                              ListContainerOptions.Builder.inDirectory("voldemort/node-0"));

            for(Entry<String, InputStream> entry: map.entrySet()) {
                System.out.println("BLAH - " + entry.getKey());
            }
            // for(StorageMetadata metadata: store.list("rsumbaly",
            // ListContainerOptions.Builder.inDirectory("voldemort/node-0"))) {
            // System.out.println("Metadata -  " + metadata.getName());
            //
            // }

        } finally {
            if(context != null) {
                context.close();
            }
        }

        return null;
    }

    /*
     * Main method for testing fetching
     */
    public static void main(String[] args) throws Exception {
        Props props = new Props();
        props.put("fetcher.identity", "AKIAIH2ZVHQDVYZUWGNA");
        props.put("fetcher.credential", "5ADjaxBgofFKZIfGXwnvpZR4q65AHrEHG2/e1dOF");
        S3Fetcher fetcher = new S3Fetcher(props);
        long start = System.currentTimeMillis();
        File location = fetcher.fetch("", System.getProperty("java.io.tmpdir") + File.separator
                                          + start);
        // double rate = size * Time.MS_PER_SECOND / (double)
        // (System.currentTimeMillis() - start);
        // NumberFormat nf = NumberFormat.getInstance();
        // nf.setMaximumFractionDigits(2);
        // System.out.println("Fetch to " + location + " completed: "
        // + nf.format(rate / (1024.0 * 1024.0)) + " MB/sec.");
    }
}

package voldemort.store.krati;

import java.io.File;

import org.apache.log4j.Logger;

import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;

public class KratiStorageConfiguration implements StorageConfiguration {

    public static final String TYPE_NAME = "krati";

    private static Logger logger = Logger.getLogger(KratiStorageConfiguration.class);

    private final String dataDirectory;
    private final int lockStripes, storeSegmentFileSizeMb, indexSegmentFileSizeMb, initLevel,
            batchSize;
    private final Object lock = new Object();

    public KratiStorageConfiguration(VoldemortConfig config) {
        Props props = config.getAllProps();
        File kratiDir = new File(config.getDataDirectory(), "krati");
        kratiDir.mkdirs();
        this.dataDirectory = kratiDir.getAbsolutePath();
        this.batchSize = props.getInt("krati.batch.size", 10000);
        this.indexSegmentFileSizeMb = props.getInt("krati.index.segment.mb", 32);
        this.storeSegmentFileSizeMb = props.getInt("krati.store.segment.mb", 256);
        this.initLevel = props.getInt("krati.init.level", 2);
        this.lockStripes = props.getInt("krati.lock.stripes", 50);

    }

    public void close() {}

    public StorageEngine<ByteArray, byte[]> getStore(String storeName) {
        synchronized(lock) {
            File storeDir = new File(dataDirectory, storeName);
            if(!storeDir.exists()) {
                logger.info("Creating krati data directory '" + storeDir.getAbsolutePath() + "'.");
                storeDir.mkdirs();
            }

            return new KratiStorageEngine(storeName,
                                          batchSize,
                                          indexSegmentFileSizeMb,
                                          storeSegmentFileSizeMb,
                                          lockStripes,
                                          initLevel,
                                          storeDir);
        }
    }

    public String getType() {
        return TYPE_NAME;
    }

}
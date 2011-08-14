/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.bdb.mr;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Take key md5s and value bytes and build a read-only store from these values
 */
@SuppressWarnings("deprecation")
public class HadoopBdbStoreBuilderReducer extends AbstractBdbStoreBuilderConfigurable implements
        Reducer<BytesWritable, BytesWritable, Text, Text> {

    private static final Logger logger = Logger.getLogger(HadoopBdbStoreBuilderReducer.class);

    private int nodeId = -1;
    private int partitionId = -1;
    private int replicaType = -1;
    private String taskId = null;

    private File localDirectory;
    private Environment environment;
    private Database database;

    private JobConf conf;

    private String outputDir;

    /**
     * Reduce should get sorted MD5 of Voldemort key ( either 16 bytes if saving
     * keys is disabled, else 8 bytes ) as key and for value (a) node-id,
     * partition-id, value - if saving keys is disabled (b) node-id,
     * partition-id, replica-type, [key-size, value-size, key, value]* if saving
     * keys is enabled
     */
    public void reduce(BytesWritable key,
                       Iterator<BytesWritable> iterator,
                       OutputCollector<Text, Text> output,
                       Reporter reporter) throws IOException {

        BytesWritable writable = iterator.next();
        byte[] valueBytes = writable.get();
        int offsetTillNow = 0;

        // Read node Id
        if(this.nodeId == -1)
            this.nodeId = ByteUtils.readInt(valueBytes, offsetTillNow);
        offsetTillNow += ByteUtils.SIZE_OF_INT;

        // Read partition id
        if(this.partitionId == -1)
            this.partitionId = ByteUtils.readInt(valueBytes, offsetTillNow);
        offsetTillNow += ByteUtils.SIZE_OF_INT;

        // Read replica type
        if(this.replicaType == -1)
            this.replicaType = (int) ByteUtils.readBytes(valueBytes,
                                                         offsetTillNow,
                                                         ByteUtils.SIZE_OF_BYTE);
        offsetTillNow += ByteUtils.SIZE_OF_BYTE;

        int keySize = ByteUtils.readInt(valueBytes, offsetTillNow);
        offsetTillNow += ByteUtils.SIZE_OF_INT;

        int valueSize = ByteUtils.readInt(valueBytes, offsetTillNow);
        offsetTillNow += ByteUtils.SIZE_OF_INT;

        byte[] finalKeyBytes = ByteUtils.copy(valueBytes, offsetTillNow, offsetTillNow + keySize);
        offsetTillNow += keySize;

        byte[] finalValueBytes = ByteUtils.copy(valueBytes, offsetTillNow, offsetTillNow
                                                                           + valueSize);

        if(iterator.hasNext())
            throw new VoldemortException("Duplicate keys detected for md5 sum "
                                         + ByteUtils.toHexString(ByteUtils.copy(key.get(),
                                                                                0,
                                                                                key.getSize())));

        database.put(null, new DatabaseEntry(finalKeyBytes), new DatabaseEntry(finalValueBytes));
    }

    /**
     * Create a temporary directory that is a child of the given directory
     * 
     * @param parent The parent directory
     * @return The temporary directory
     */
    public File createTempDir() {
        File temp = new File(System.getProperty("java.io.tmpdir"),
                             Integer.toString(new Random().nextInt()));
        temp.delete();
        temp.mkdir();
        temp.deleteOnExit();
        return temp;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        this.conf = job;
        this.outputDir = job.get("final.output.dir");
        this.taskId = job.get("mapred.task.id");

        // Create local file for environment
        File tempLocalFile = createTempDir();

        // Create data-base directory
        this.localDirectory = new File(tempLocalFile, this.taskId);
        Utils.mkdirs(localDirectory);

        // Create directory for
        // Set the environment config
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION,
                                         Long.toString(80));
        environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, Boolean.toString(false));
        environmentConfig.setTransactional(false);
        environmentConfig.setLocking(false);
        environmentConfig.setReadOnly(false);
        environmentConfig.setAllowCreate(true);

        // Set the database config
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setReadOnly(false);
        dbConfig.setDeferredWrite(true);

        // Open the environment
        environment = new Environment(localDirectory, environmentConfig);

        // Open the DB
        database = environment.openDatabase(null, "voldemort", dbConfig);

        logger.info("Opened database at local location " + localDirectory);
    }

    @Override
    public void close() throws IOException {

        if(this.nodeId == -1 && this.replicaType == -1 && this.partitionId == -1) {
            // Issue 258 - No data was read in the reduce phase, do not create
            // any output
            return;
        }

        // Flush and close the BDB environment
        logger.info("Syncing environment at " + environment.getHome().getPath());
        environment.sync();
        logger.info("Done syncing environment at " + environment.getHome().getPath());

        logger.info("Cleaning environment log at " + environment.getHome().getPath());
        boolean anyCleaned = false;
        while(environment.cleanLog() > 0) {
            anyCleaned = true;
        }
        logger.info("Done cleaning environment log at " + environment.getHome().getPath());
        if(anyCleaned) {
            logger.info("Checkpointing environment at " + environment.getHome().getPath());
            CheckpointConfig checkpoint = new CheckpointConfig();
            checkpoint.setForce(true);
            environment.checkpoint(checkpoint);
            logger.info("Done checkpointing environment at " + environment.getHome().getPath());
        }

        database.close();
        environment.close();

        // Initialize the node directory
        Path nodeDir = new Path(this.outputDir, "node-" + this.nodeId);

        // Initialize the partition folder
        Path partitionDir = new Path(nodeDir, new String(Integer.toString(this.partitionId) + "_"
                                                         + Integer.toString(this.replicaType)));

        // Create output directory, if it doesn't exist
        FileSystem outputFs = nodeDir.getFileSystem(this.conf);
        outputFs.mkdirs(nodeDir);

        // Create the partition folder
        outputFs.mkdirs(partitionDir);

        logger.info("Final output directory - " + partitionDir);

        // Local path
        Path localPath = new Path(localDirectory.getAbsolutePath());

        logger.info("Moving " + localPath + " to " + partitionDir);
        outputFs.copyFromLocalFile(localPath, partitionDir);

    }
}

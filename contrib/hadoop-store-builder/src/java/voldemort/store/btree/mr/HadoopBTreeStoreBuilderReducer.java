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

package voldemort.store.btree.mr;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.readonly.ReadOnlyUtils;
import voldemort.store.readonly.mr.HadoopStoreBuilderReducer;
import voldemort.utils.ByteUtils;

import com.google.common.collect.Maps;

/**
 * Take key md5s and value bytes and build a read-only store from these values
 */
@SuppressWarnings("deprecation")
public class HadoopBTreeStoreBuilderReducer extends AbstractBTreeStoreBuilderConfigurable implements
        Reducer<BytesWritable, BytesWritable, Text, Text> {

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilderReducer.class);

    private DataOutputStream valueFileStream = null;
    private int position;
    private String taskId = null;

    private int nodeId = -1;
    private int partitionId = -1;
    private int chunkId = -1;
    private int replicaType = -1;

    private Path outputPath;
    private Path taskValueFileName;

    private long maxLevel;
    private HashMap<Long, Path> indexPaths;
    private HashMap<Long, DataOutputStream> indexStreams;
    private HashMap<Long, Long> indexBlockSizePerLevel;
    private HashMap<Long, Long> indexOffsetPerLevel;

    private JobConf conf;

    private String outputDir;

    private FileSystem fs;

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

        // Go over every level and make some decisions
        long currentLevel = 0;
        boolean updateForNextLevel = false;
        long previousLevelOffset = this.position;
        do {
            DataOutputStream stream = this.indexStreams.get(currentLevel);
            stream.write(key.get(), 0, key.getSize());
            stream.writeInt((int) previousLevelOffset);

            // Bump up the group size
            Long currentBlockSize = indexBlockSizePerLevel.get(currentLevel);
            currentBlockSize++;
            indexBlockSizePerLevel.put(currentLevel, currentBlockSize);

            // Increment the offset
            Long currentOffset = indexOffsetPerLevel.get(currentLevel);
            previousLevelOffset = currentOffset.longValue();
            currentOffset += (8 + ByteUtils.SIZE_OF_INT);
            indexOffsetPerLevel.put(currentLevel, currentOffset);

            if(currentBlockSize > 512) {
                currentBlockSize = 1L;
                indexBlockSizePerLevel.put(currentLevel, currentBlockSize);
                updateForNextLevel = true;

                // Check if upper level has ever existed, if not create
                // it
                if(currentLevel == maxLevel) {
                    maxLevel++;
                    currentLevel++;
                    indexBlockSizePerLevel.put(currentLevel, 0L);
                    indexOffsetPerLevel.put(currentLevel, 0L);

                    Path indexFile = new Path(outputPath, getStoreName() + "." + this.taskId
                                                          + ".index." + currentLevel);
                    this.indexPaths.put(currentLevel, indexFile);
                    this.indexStreams.put(currentLevel, fs.create(indexFile));
                } else {
                    currentLevel++;
                }
            } else {
                updateForNextLevel = false;
            }
        } while(currentLevel <= maxLevel && updateForNextLevel);

        short numTuples = 0;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream valueStream = new DataOutputStream(stream);

        while(iterator.hasNext()) {
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

            // Read chunk id
            if(this.chunkId == -1)
                this.chunkId = ReadOnlyUtils.chunk(key.get(), getNumChunks());

            // Read replica type
            if(this.replicaType == -1)
                this.replicaType = (int) ByteUtils.readBytes(valueBytes,
                                                             offsetTillNow,
                                                             ByteUtils.SIZE_OF_BYTE);
            offsetTillNow += ByteUtils.SIZE_OF_BYTE;

            int valueLength = writable.getSize() - offsetTillNow;
            // Write ( key_length, value_length, key,
            // value )
            valueStream.write(valueBytes, offsetTillNow, valueLength);

            numTuples++;

        }

        // Flush the value
        valueStream.flush();
        byte[] value = stream.toByteArray();

        // Start writing to file now
        // First, if save keys flag set the number of keys

        this.valueFileStream.writeShort(numTuples);
        this.position += ByteUtils.SIZE_OF_SHORT;

        this.valueFileStream.write(value);
        this.position += value.length;

        if(this.position < 0)
            throw new VoldemortException("Chunk overflow exception: chunk " + chunkId
                                         + " has exceeded " + Integer.MAX_VALUE + " bytes.");

    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        try {
            this.conf = job;
            this.position = 0;
            this.outputDir = job.get("final.output.dir");
            this.taskId = job.get("mapred.task.id");

            this.outputPath = FileOutputFormat.getOutputPath(job);
            this.taskValueFileName = new Path(outputPath, getStoreName() + "." + this.taskId
                                                          + ".data");
            if(this.fs == null)
                this.fs = this.taskValueFileName.getFileSystem(job);

            this.valueFileStream = fs.create(this.taskValueFileName);

            this.maxLevel = 0;

            Path indexFile = new Path(outputPath, getStoreName() + "." + this.taskId + ".index."
                                                  + maxLevel);
            this.indexPaths = Maps.newHashMap();
            this.indexPaths.put(maxLevel, indexFile);

            this.indexStreams = Maps.newHashMap();
            this.indexStreams.put(maxLevel, fs.create(indexFile));

            this.indexOffsetPerLevel = Maps.newHashMap();
            this.indexOffsetPerLevel.put(maxLevel, 0L);

            this.indexBlockSizePerLevel = Maps.newHashMap();
            this.indexBlockSizePerLevel.put(maxLevel, 0L);

            logger.info("Opening " + this.taskValueFileName + " and " + this.indexPaths.get(0L)
                        + " for writing.");

        } catch(IOException e) {
            throw new RuntimeException("Failed to open Input/OutputStream", e);
        }
    }

    @Override
    public void close() throws IOException {

        this.valueFileStream.flush();
        this.valueFileStream.close();

        // Similarly flush and close the indexes
        for(DataOutputStream stream: indexStreams.values()) {
            stream.flush();
            stream.close();
        }

        if(this.nodeId == -1 || this.chunkId == -1 || this.partitionId == -1) {
            // Issue 258 - No data was read in the reduce phase, do not create
            // any output
            return;
        }

        // If the replica type read was not valid, shout out
        if(this.replicaType == -1) {
            throw new RuntimeException("Could not read the replica type correctly for node "
                                       + nodeId + " ( partition - " + this.partitionId + " )");
        }

        String fileNamePrefix = new String(Integer.toString(this.partitionId) + "_"
                                           + Integer.toString(this.replicaType) + "_"
                                           + Integer.toString(this.chunkId));

        // Initialize the node directory
        Path nodeDir = new Path(this.outputDir, "node-" + this.nodeId);

        // Create output directory, if it doesn't exist
        FileSystem outputFs = nodeDir.getFileSystem(this.conf);
        outputFs.mkdirs(nodeDir);

        // First flush and close the data file
        Path valueFile = new Path(nodeDir, fileNamePrefix + ".data");
        logger.info("Moving " + this.taskValueFileName + " to " + valueFile);
        outputFs.rename(this.taskValueFileName, valueFile);

        // Move all the index file
        for(Entry<Long, Path> inputFilePerLevel: indexPaths.entrySet()) {
            Path indexFile = new Path(nodeDir, fileNamePrefix + ".index."
                                               + inputFilePerLevel.getKey());
            logger.info("Moving " + inputFilePerLevel.getValue() + " to " + indexFile);
            outputFs.rename(inputFilePerLevel.getValue(), indexFile);
        }

    }
}

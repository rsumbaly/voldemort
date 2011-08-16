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

package voldemort.store.readonly.mr;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.Compression;
import voldemort.store.StoreDefinition;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Builds a read-only voldemort store as a hadoop job from the given input data.
 * 
 */
@SuppressWarnings("deprecation")
public class HadoopStoreBuilder {

    public static final long MIN_CHUNK_SIZE = 1L;
    public static final long MAX_CHUNK_SIZE = (long) (1.9 * 1024 * 1024 * 1024);
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    public static final int NUM_ATTEMPTS = 2;

    private static final Logger logger = Logger.getLogger(HadoopStoreBuilder.class);

    private final Configuration config;
    private final Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass;
    @SuppressWarnings("unchecked")
    private final Class<? extends InputFormat> inputFormatClass;
    private final Cluster cluster;
    private final StoreDefinition storeDef;
    private final long chunkSizeBytes;
    private final Path inputPath;
    private final Path outputDir;
    private final Path tempDir;
    private CheckSumType checkSumType = CheckSumType.NONE;
    private Compression compression = null;
    private boolean saveKeys = false;
    private boolean reducerPerBucket = false;
    private int numChunks = -1;

    /**
     * Kept for backwards compatibility. We do not use replicationFactor any
     * more since it is derived from the store definition
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param replicationFactor NOT USED
     * @param chunkSizeBytes The size of the chunks used by the read-only store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              int replicationFactor,
                              long chunkSizeBytes,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath) {
        this(conf,
             mapperClass,
             inputFormatClass,
             cluster,
             storeDef,
             chunkSizeBytes,
             tempDir,
             outputDir,
             inputPath);
    }

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param chunkSizeBytes The size of the chunks used by the read-only store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              long chunkSizeBytes,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath) {
        super();
        this.config = conf;
        this.mapperClass = Utils.notNull(mapperClass);
        this.inputFormatClass = Utils.notNull(inputFormatClass);
        this.inputPath = inputPath;
        this.cluster = Utils.notNull(cluster);
        this.storeDef = Utils.notNull(storeDef);
        this.chunkSizeBytes = chunkSizeBytes;
        this.tempDir = tempDir;
        this.outputDir = Utils.notNull(outputDir);
        if(chunkSizeBytes > MAX_CHUNK_SIZE || chunkSizeBytes < MIN_CHUNK_SIZE)
            throw new VoldemortException("Invalid chunk size, chunk size must be in the range "
                                         + MIN_CHUNK_SIZE + "..." + MAX_CHUNK_SIZE);
    }

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param chunkSizeBytes The size of the chunks used by the read-only store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     * @param checkSumType The checksum algorithm to use
     * @param saveKeys Boolean to signify if we want to save the key as well
     * @param reducerPerBucket Boolean to signify whether we want to have a
     *        single reducer for a bucket ( thereby resulting in all chunk files
     *        for a bucket being generated in a single reducer )
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              long chunkSizeBytes,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath,
                              CheckSumType checkSumType,
                              boolean saveKeys,
                              boolean reducerPerBucket) {
        this(conf,
             mapperClass,
             inputFormatClass,
             cluster,
             storeDef,
             chunkSizeBytes,
             tempDir,
             outputDir,
             inputPath,
             checkSumType,
             null,
             saveKeys,
             reducerPerBucket);
    }

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     * @param checkSumType The checksum algorithm to use
     * @param saveKeys Boolean to signify if we want to save the key as well
     * @param reducerPerBucket Boolean to signify whether we want to have a
     *        single reducer for a bucket ( thereby resulting in all chunk files
     *        for a bucket being generated in a single reducer )
     * @param numChunks Number of chunks per bucket ( partition or partition
     *        replica )
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath,
                              CheckSumType checkSumType,
                              boolean saveKeys,
                              boolean reducerPerBucket,
                              int numChunks) {
        this(conf,
             mapperClass,
             inputFormatClass,
             cluster,
             storeDef,
             tempDir,
             outputDir,
             inputPath,
             checkSumType,
             null,
             saveKeys,
             reducerPerBucket,
             numChunks);
    }

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param chunkSizeBytes The size of the chunks used by the read-only store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     * @param checkSumType The checksum algorithm to use
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              long chunkSizeBytes,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath,
                              CheckSumType checkSumType) {
        this(conf,
             mapperClass,
             inputFormatClass,
             cluster,
             storeDef,
             chunkSizeBytes,
             tempDir,
             outputDir,
             inputPath);
        this.checkSumType = checkSumType;

    }

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param chunkSizeBytes The size of the chunks used by the read-only store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     * @param checkSumType The checksum algorithm to use
     * @param compression Type of compression ( can be null )
     * @param saveKeys Boolean to signify if we want to save the key as well
     * @param reducerPerBucket Boolean to signify whether we want to have a
     *        single reducer for a bucket ( thereby resulting in all chunk files
     *        for a bucket being generated in a single reducer )
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              long chunkSizeBytes,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath,
                              CheckSumType checkSumType,
                              Compression compression,
                              boolean saveKeys,
                              boolean reducerPerBucket) {
        this(conf,
             mapperClass,
             inputFormatClass,
             cluster,
             storeDef,
             chunkSizeBytes,
             tempDir,
             outputDir,
             inputPath,
             checkSumType);
        this.compression = compression;
        this.saveKeys = saveKeys;
        this.reducerPerBucket = reducerPerBucket;
    }

    /**
     * Create the store builder
     * 
     * @param conf A base configuration to start with
     * @param mapperClass The class to use as the mapper
     * @param inputFormatClass The input format to use for reading values
     * @param cluster The voldemort cluster for which the stores are being built
     * @param storeDef The store definition of the store
     * @param tempDir The temporary directory to use in hadoop for intermediate
     *        reducer output
     * @param outputDir The directory in which to place the built stores
     * @param inputPath The path from which to read input data
     * @param checkSumType The checksum algorithm to use
     * @param compression Type of compression ( can be null )
     * @param saveKeys Boolean to signify if we want to save the key as well
     * @param reducerPerBucket Boolean to signify whether we want to have a
     *        single reducer for a bucket ( thereby resulting in all chunk files
     *        for a bucket being generated in a single reducer )
     * @param numChunks Number of chunks per bucket ( partition or partition
     *        replica )
     */
    @SuppressWarnings("unchecked")
    public HadoopStoreBuilder(Configuration conf,
                              Class<? extends AbstractHadoopStoreBuilderMapper<?, ?>> mapperClass,
                              Class<? extends InputFormat> inputFormatClass,
                              Cluster cluster,
                              StoreDefinition storeDef,
                              Path tempDir,
                              Path outputDir,
                              Path inputPath,
                              CheckSumType checkSumType,
                              Compression compression,
                              boolean saveKeys,
                              boolean reducerPerBucket,
                              int numChunks) {
        super();
        this.config = conf;
        this.mapperClass = Utils.notNull(mapperClass);
        this.inputFormatClass = Utils.notNull(inputFormatClass);
        this.inputPath = inputPath;
        this.cluster = Utils.notNull(cluster);
        this.storeDef = Utils.notNull(storeDef);
        this.chunkSizeBytes = -1;
        this.tempDir = tempDir;
        this.outputDir = Utils.notNull(outputDir);
        this.checkSumType = checkSumType;
        this.compression = compression;
        this.saveKeys = saveKeys;
        this.reducerPerBucket = reducerPerBucket;
        this.numChunks = numChunks;
        if(numChunks <= 0)
            throw new VoldemortException("Number of chunks should be greater than zero");
    }

    /**
     * Run the job
     */
    public void build() {
        try {
            RunningJob job = null;
            boolean tryAgain = false;
            int attemptId = 1;

            JobConf conf = new JobConf(config);
            FileSystem outputFs = outputDir.getFileSystem(conf);
            if(outputFs.exists(outputDir)) {
                throw new IOException("Final output directory already exists.");
            }

            CompressionStrategy strategy = null;
            boolean compressionExists = true;

            // Do compression only if we haven't done a row level
            // compression
            if(compression != null && storeDef.getValueSerializer().getCompression() == null
               && storeDef.getKeySerializer().getCompression() == null) {

                // Create a compression factory to check if the algorithm
                // exists
                try {
                    strategy = new CompressionStrategyFactory().get(compression);
                    logger.info("Successful instantiated compression type " + compression.getType());
                } catch(Exception e) {
                    logger.warn("Could not instantiate compression type ( " + compression.getType()
                                + ") successfully. Continuing without any compression");
                    compressionExists = false;
                }

            }

            do {
                conf.setInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
                conf.set("cluster.xml", new ClusterMapper().writeCluster(cluster));
                conf.set("stores.xml",
                         new StoreDefinitionsMapper().writeStoreList(Collections.singletonList(storeDef)));
                conf.setBoolean("save.keys", saveKeys);
                conf.setBoolean("reducer.per.bucket", reducerPerBucket);
                conf.setPartitionerClass(HadoopStoreBuilderPartitioner.class);
                conf.setMapperClass(mapperClass);
                conf.setMapOutputKeyClass(BytesWritable.class);
                conf.setMapOutputValueClass(BytesWritable.class);
                if(reducerPerBucket) {
                    conf.setReducerClass(HadoopStoreBuilderReducerPerBucket.class);
                } else {
                    conf.setReducerClass(HadoopStoreBuilderReducer.class);
                }
                if(compressionExists && strategy != null) {
                    conf.set("push.compress", compression.getType());
                }

                conf.setInputFormat(inputFormatClass);
                conf.setOutputFormat(SequenceFileOutputFormat.class);
                conf.setOutputKeyClass(BytesWritable.class);
                conf.setOutputValueClass(BytesWritable.class);
                conf.setJarByClass(getClass());
                conf.setReduceSpeculativeExecution(false);
                conf.setMaxReduceAttempts(1);
                FileInputFormat.setInputPaths(conf, inputPath);
                conf.set("final.output.dir", outputDir.toString());
                conf.set("checksum.type", CheckSum.toString(checkSumType));
                FileOutputFormat.setOutputPath(conf, tempDir);

                // delete output dir if it already exists
                FileSystem tempFs = tempDir.getFileSystem(conf);
                tempFs.delete(tempDir, true);

                long size = sizeOfPath(tempFs, inputPath);
                logger.info("Data size = " + size + ", replication factor = "
                            + storeDef.getReplicationFactor() + ", numNodes = "
                            + cluster.getNumberOfNodes() + ", chunk size = " + chunkSizeBytes);

                // Derive "rough" number of chunks and reducers
                int numReducers;
                if(saveKeys) {
                    if(this.numChunks == -1) {
                        this.numChunks = Math.max((int) (storeDef.getReplicationFactor() * size
                                                         / cluster.getNumberOfPartitions()
                                                         / storeDef.getReplicationFactor() / chunkSizeBytes),
                                                  1);
                    } else {
                        logger.info("Overriding chunk size byte and taking num chunks ("
                                    + this.numChunks + ") directly");
                    }
                    if(reducerPerBucket) {
                        numReducers = cluster.getNumberOfPartitions()
                                      * storeDef.getReplicationFactor();
                    } else {
                        numReducers = cluster.getNumberOfPartitions()
                                      * storeDef.getReplicationFactor() * numChunks;
                    }
                } else {

                    if(this.numChunks == -1) {
                        this.numChunks = Math.max((int) (storeDef.getReplicationFactor() * size
                                                         / cluster.getNumberOfPartitions() / chunkSizeBytes),
                                                  1);
                    } else {
                        logger.info("Overriding chunk size byte and taking num chunks ("
                                    + this.numChunks + ") directly");
                    }
                    if(reducerPerBucket) {
                        numReducers = cluster.getNumberOfPartitions();
                    } else {
                        numReducers = cluster.getNumberOfPartitions() * numChunks;
                    }
                }
                conf.setInt("num.chunks", numChunks);
                conf.setNumReduceTasks(numReducers);

                logger.info("Attempt " + attemptId + "] Number of chunks: " + numChunks
                            + ", number of reducers: " + numReducers + ", save keys: " + saveKeys
                            + ", reducer per bucket: " + reducerPerBucket);
                logger.info("Building store...");
                try {
                    job = JobClient.runJob(conf);
                    tryAgain = false;
                } catch(Exception e) {
                    logger.error("Got exception during attempt " + attemptId
                                 + ". Trying again with chunks = " + (numChunks + 1), e);
                    attemptId++;
                    if(attemptId <= NUM_ATTEMPTS) {
                        tryAgain = true;
                    } else {
                        logger.error("Completed " + NUM_ATTEMPTS
                                     + " attempts. Failing the full job");
                        throw e;
                    }
                }
                numChunks++;
            } while(tryAgain);

            // Once the job has completed log the counter
            Counters counters = job.getCounters();

            if(saveKeys) {
                if(reducerPerBucket) {
                    logger.info("Number of collisions in the job - "
                                + counters.getCounter(HadoopStoreBuilderReducerPerBucket.CollisionCounter.NUM_COLLISIONS));
                    logger.info("Maximum number of collisions for one entry - "
                                + counters.getCounter(HadoopStoreBuilderReducerPerBucket.CollisionCounter.MAX_COLLISIONS));
                } else {
                    logger.info("Number of collisions in the job - "
                                + counters.getCounter(HadoopStoreBuilderReducer.CollisionCounter.NUM_COLLISIONS));
                    logger.info("Maximum number of collisions for one entry - "
                                + counters.getCounter(HadoopStoreBuilderReducer.CollisionCounter.MAX_COLLISIONS));
                }
            }

            // Do a CheckSumOfCheckSum - Similar to HDFS
            CheckSum checkSumGenerator = CheckSum.getInstance(this.checkSumType);
            if(!this.checkSumType.equals(CheckSumType.NONE) && checkSumGenerator == null) {
                throw new VoldemortException("Could not generate checksum digest for type "
                                             + this.checkSumType);
            }

            // Check if all folder exists and with format file
            for(Node node: cluster.getNodes()) {

                ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();

                if(saveKeys) {
                    metadata.add(ReadOnlyStorageMetadata.FORMAT,
                                 ReadOnlyStorageFormat.READONLY_V2.getCode());
                } else {
                    metadata.add(ReadOnlyStorageMetadata.FORMAT,
                                 ReadOnlyStorageFormat.READONLY_V1.getCode());
                }

                Path nodePath = new Path(outputDir.toString(), "node-" + node.getId());

                if(!outputFs.exists(nodePath)) {
                    logger.info("No data generated for node " + node.getId()
                                + ". Generating empty folder");
                    outputFs.mkdirs(nodePath); // Create empty folder
                }

                // Check if we need to do checkum
                if(checkSumType != CheckSumType.NONE) {

                    FileStatus[] storeFiles = outputFs.listStatus(nodePath, new PathFilter() {

                        public boolean accept(Path arg0) {
                            if(arg0.getName().endsWith("checksum")
                               && !arg0.getName().startsWith(".")) {
                                return true;
                            }
                            return false;
                        }
                    });

                    if(storeFiles != null && storeFiles.length > 0) {
                        Arrays.sort(storeFiles, new IndexFileLastComparator());
                        FSDataInputStream input = null;

                        for(FileStatus file: storeFiles) {
                            try {
                                input = outputFs.open(file.getPath());
                                byte fileCheckSum[] = new byte[CheckSum.checkSumLength(this.checkSumType)];
                                input.read(fileCheckSum);
                                logger.debug("Checksum for file " + file.toString() + " - "
                                             + new String(Hex.encodeHex(fileCheckSum)));
                                checkSumGenerator.update(fileCheckSum);
                            } catch(Exception e) {
                                logger.error("Error while reading checksum file " + e.getMessage(),
                                             e);
                            } finally {
                                if(input != null)
                                    input.close();
                            }
                            outputFs.delete(file.getPath(), false);
                        }

                        metadata.add(ReadOnlyStorageMetadata.CHECKSUM_TYPE,
                                     CheckSum.toString(checkSumType));

                        String checkSum = new String(Hex.encodeHex(checkSumGenerator.getCheckSum()));
                        logger.info("Checksum for node " + node.getId() + " - " + checkSum);

                        metadata.add(ReadOnlyStorageMetadata.CHECKSUM, checkSum);
                    }
                }

                // Check if we this data was compresses
                if(strategy != null) {
                    // Add this information to the metadata file
                    metadata.add(ReadOnlyStorageMetadata.COMPRESSION_TYPE, strategy.getType());
                }

                // Write metadata
                FSDataOutputStream metadataStream = outputFs.create(new Path(nodePath, ".metadata"));
                metadataStream.write(metadata.toJsonString().getBytes());
                metadataStream.flush();
                metadataStream.close();

            }

        } catch(Exception e) {
            logger.error("Error in Store builder", e);
            throw new VoldemortException(e);
        }

    }

    /**
     * A comparator that sorts index files last. This is required to maintain
     * the order while calculating checksum
     * 
     */
    static class IndexFileLastComparator implements Comparator<FileStatus> {

        public int compare(FileStatus fs1, FileStatus fs2) {
            // directories before files
            if(fs1.isDir())
                return fs2.isDir() ? 0 : -1;
            if(fs2.isDir())
                return fs1.isDir() ? 0 : 1;

            String f1 = fs1.getPath().getName(), f2 = fs2.getPath().getName();

            // if both same, lexicographically
            if((f1.contains(".index") && f2.contains(".index"))
               || (f1.contains(".data") && f2.contains(".data"))) {
                return f1.compareToIgnoreCase(f2);
            }

            if(f1.contains(".index")) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    private long sizeOfPath(FileSystem fs, Path path) throws IOException {
        long size = 0;
        FileStatus[] statuses = fs.listStatus(path);
        if(statuses != null) {
            for(FileStatus status: statuses) {
                if(status.isDir())
                    size += sizeOfPath(fs, status.getPath());
                else
                    size += status.getLen();
            }
        }
        return size;
    }

}

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

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyStorageFormat;
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.utils.Utils;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * Builds a read-only voldemort store as a hadoop job from the given input data.
 * 
 */
@SuppressWarnings("deprecation")
public class HadoopBdbStoreBuilder {

    public static final long MIN_CHUNK_SIZE = 1L;
    public static final long MAX_CHUNK_SIZE = (long) (1.9 * 1024 * 1024 * 1024);
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private static final Logger logger = Logger.getLogger(HadoopBdbStoreBuilder.class);

    private final Configuration config;
    private final Class<? extends AbstractHadoopBdbStoreBuilderMapper<?, ?>> mapperClass;
    @SuppressWarnings("unchecked")
    private final Class<? extends InputFormat> inputFormatClass;
    private final Cluster cluster;
    private final StoreDefinition storeDef;
    private final Path inputPath;
    private final Path outputDir;
    private final Path tempDir;

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
     */
    @SuppressWarnings("unchecked")
    public HadoopBdbStoreBuilder(Configuration conf,
                                 Class<? extends AbstractHadoopBdbStoreBuilderMapper<?, ?>> mapperClass,
                                 Class<? extends InputFormat> inputFormatClass,
                                 Cluster cluster,
                                 StoreDefinition storeDef,
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
        this.tempDir = tempDir;
        this.outputDir = Utils.notNull(outputDir);
    }

    /**
     * Run the job
     */
    public void build() {
        try {
            JobConf conf = new JobConf(config);
            conf.setInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
            conf.set("cluster.xml", new ClusterMapper().writeCluster(cluster));
            conf.set("stores.xml",
                     new StoreDefinitionsMapper().writeStoreList(Collections.singletonList(storeDef)));
            conf.setPartitionerClass(HadoopBdbStoreBuilderPartitioner.class);
            conf.setMapperClass(mapperClass);
            conf.setMapOutputKeyClass(BytesWritable.class);
            conf.setMapOutputValueClass(BytesWritable.class);
            conf.setReducerClass(HadoopBdbStoreBuilderReducer.class);
            conf.setInputFormat(inputFormatClass);
            conf.setOutputFormat(SequenceFileOutputFormat.class);
            conf.setOutputKeyClass(BytesWritable.class);
            conf.setOutputValueClass(BytesWritable.class);
            conf.setJarByClass(getClass());
            conf.setReduceSpeculativeExecution(false);
            FileInputFormat.setInputPaths(conf, inputPath);
            conf.set("final.output.dir", outputDir.toString());
            FileOutputFormat.setOutputPath(conf, tempDir);

            FileSystem outputFs = outputDir.getFileSystem(conf);
            if(outputFs.exists(outputDir)) {
                throw new IOException("Final output directory already exists.");
            }

            // delete output dir if it already exists
            FileSystem tempFs = tempDir.getFileSystem(conf);
            tempFs.delete(tempDir, true);

            long size = sizeOfPath(tempFs, inputPath);
            logger.info("Data size = " + size + ", replication factor = "
                        + storeDef.getReplicationFactor() + ", numNodes = "
                        + cluster.getNumberOfNodes());

            // Derive "rough" number of chunks and reducers
            int numReducers = cluster.getNumberOfPartitions() * storeDef.getReplicationFactor();
            conf.setNumReduceTasks(numReducers);

            logger.info("Number of reducer: " + numReducers);
            logger.info("Building store...");
            JobClient.runJob(conf);

            // Check if all folder exists and with format file
            for(Node node: cluster.getNodes()) {

                ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata();

                metadata.add(ReadOnlyStorageMetadata.FORMAT, ReadOnlyStorageFormat.BDB.getCode());

                Path nodePath = new Path(outputDir.toString(), "node-" + node.getId());

                if(!outputFs.exists(nodePath)) {
                    logger.info("No data generated for node " + node.getId()
                                + ". Generating empty folder");
                    outputFs.mkdirs(nodePath); // Create empty folder
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

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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;

/**
 * Unit test to check Read-Only Batch Indexer <strong>in Local mode numReduce
 * will be only one hence we will see only one node files irrespective of
 * cluster details.</strong>
 * 
 * 
 */
@SuppressWarnings("deprecation")
public class HadoopBdbStoreBuilderTest {

    public static class TextStoreMapper extends
            AbstractHadoopBdbStoreBuilderMapper<LongWritable, Text> {

        @Override
        public Object makeKey(LongWritable key, Text value) {
            String[] tokens = value.toString().split("\\s+");
            return tokens[0];
        }

        @Override
        public Object makeValue(LongWritable key, Text value) {
            String[] tokens = value.toString().split("\\s+");
            return tokens[1];
        }

    }

    @Test
    public void testHadoopBuild() throws Exception {
        // create test data
        Map<String, String> values = new HashMap<String, String>();
        File testDir = TestUtils.createTempDir();
        File tempDir = new File(testDir, "temp");
        File outputDir = new File(testDir, "output");
        for(int i = 0; i < 5000; i++)
            values.put(Integer.toString(i), Integer.toBinaryString(i));

        // write test data to text file
        File inputFile = File.createTempFile("input", ".txt", testDir);
        inputFile.deleteOnExit();
        StringBuilder contents = new StringBuilder();
        for(Map.Entry<String, String> entry: values.entrySet())
            contents.append(entry.getKey() + "\t" + entry.getValue() + "\n");
        FileUtils.writeStringToFile(inputFile, contents.toString());

        String storeName = "test";
        SerializerDefinition serDef = new SerializerDefinition("string");
        Cluster cluster = ServerTestUtils.getLocalCluster(1);

        // Test backwards compatibility
        StoreDefinition def = new StoreDefinitionBuilder().setName(storeName)
                                                          .setType(ReadOnlyStorageConfiguration.TYPE_NAME)
                                                          .setKeySerializer(serDef)
                                                          .setValueSerializer(serDef)
                                                          .setRoutingPolicy(RoutingTier.CLIENT)
                                                          .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                          .setReplicationFactor(1)
                                                          .setPreferredReads(1)
                                                          .setRequiredReads(1)
                                                          .setPreferredWrites(1)
                                                          .setRequiredWrites(1)
                                                          .build();
        HadoopBdbStoreBuilder builder = new HadoopBdbStoreBuilder(new Configuration(),
                                                                  TextStoreMapper.class,
                                                                  TextInputFormat.class,
                                                                  cluster,
                                                                  def,
                                                                  new Path(tempDir.getAbsolutePath()),
                                                                  new Path(outputDir.getAbsolutePath()),
                                                                  new Path(inputFile.getAbsolutePath()));
        builder.build();

    }
}

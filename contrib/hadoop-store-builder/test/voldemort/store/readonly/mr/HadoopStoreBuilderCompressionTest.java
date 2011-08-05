package voldemort.store.readonly.mr;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.RoutingTier;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.Compression;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.mr.HadoopStoreBuilderTest.TextStoreMapper;

@RunWith(Parameterized.class)
public class HadoopStoreBuilderCompressionTest {

    private Compression compression;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { new Compression("gzip", null) },
                { new Compression("snappy", null) }, { new Compression("lzf", null) } });
    }

    public HadoopStoreBuilderCompressionTest(Compression compression) {
        this.compression = compression;
    }

    @Test
    public void testHadoopBuild() throws Exception {
        // create test data
        Map<String, String> values = new HashMap<String, String>();
        File testDir = TestUtils.createTempDir();
        File tempDir = new File(testDir, "temp"), tempDir2 = new File(testDir, "temp2");
        File outputDir = new File(testDir, "output"), outputDir2 = new File(testDir, "output2");
        File storeDir = TestUtils.createTempDir(testDir);
        for(int i = 0; i < 200; i++)
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
        HadoopStoreBuilder builder = new HadoopStoreBuilder(new Configuration(),
                                                            TextStoreMapper.class,
                                                            TextInputFormat.class,
                                                            cluster,
                                                            def,
                                                            64 * 1024,
                                                            new Path(tempDir.getAbsolutePath()),
                                                            new Path(outputDir.getAbsolutePath()),
                                                            new Path(inputFile.getAbsolutePath()),
                                                            CheckSumType.MD5,
                                                            null,
                                                            true,
                                                            false);
        builder.build();

        builder = new HadoopStoreBuilder(new Configuration(),
                                         TextStoreMapper.class,
                                         TextInputFormat.class,
                                         cluster,
                                         def,
                                         64 * 1024,
                                         new Path(tempDir2.getAbsolutePath()),
                                         new Path(outputDir2.getAbsolutePath()),
                                         new Path(inputFile.getAbsolutePath()),
                                         CheckSumType.MD5,
                                         compression,
                                         true,
                                         false);
        builder.build();
    }
}

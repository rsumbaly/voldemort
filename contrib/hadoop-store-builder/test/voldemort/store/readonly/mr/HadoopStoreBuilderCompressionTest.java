package voldemort.store.readonly.mr;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.Assert;
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
import voldemort.store.readonly.ReadOnlyStorageMetadata;
import voldemort.store.readonly.checksum.CheckSumTests;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;
import voldemort.store.readonly.fetcher.HdfsFetcher;
import voldemort.store.readonly.mr.HadoopStoreBuilderTest.TextStoreMapper;
import voldemort.utils.ByteUtils;

@SuppressWarnings("deprecation")
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
        for(int i = 0; i < 400; i++)
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

        // Check if checkSum is generated in outputDir
        File nodeFile = new File(outputDir, "node-0");

        // Check if metadata file exists
        File metadataFile = new File(nodeFile, ".metadata");
        Assert.assertTrue(metadataFile.exists());

        // Read the metadata
        ReadOnlyStorageMetadata metadata = new ReadOnlyStorageMetadata(metadataFile);

        // Check contents of checkSum file
        byte[] md5 = Hex.decodeHex(((String) metadata.get(ReadOnlyStorageMetadata.CHECKSUM)).toCharArray());
        byte[] checkSumBytes = CheckSumTests.calculateCheckSum(nodeFile.listFiles(),
                                                               CheckSumType.MD5);
        Assert.assertEquals(0, ByteUtils.compare(checkSumBytes, md5));

        // check if fetching works
        HdfsFetcher fetcher = new HdfsFetcher();

        // Fetch to version directory
        File versionDir = new File(storeDir, "version-0");
        File fetchedDir1 = fetcher.fetch(nodeFile.getAbsolutePath(), versionDir.getAbsolutePath());
        Assert.assertTrue(fetchedDir1.getName().compareTo("version-0") == 0);

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

        // Fetch to version directory
        nodeFile = new File(outputDir2, "node-0");
        versionDir = new File(storeDir, "version-1");
        File fetchedDir2 = fetcher.fetch(nodeFile.getAbsolutePath(), versionDir.getAbsolutePath());
        Assert.assertTrue(fetchedDir2.getName().compareTo("version-1") == 0);

        // Make sure all the files ( except metadata ) are same in fetchedDir1
        // and fetchedDir2

        for(File file: fetchedDir1.listFiles()) {
            if(!file.getName().contains("metadata")) {
                File otherFile = new File(fetchedDir2, file.getName());
                Assert.assertTrue(otherFile.exists());
                Assert.assertEquals(otherFile.length(), file.length());
            }
        }
    }
}

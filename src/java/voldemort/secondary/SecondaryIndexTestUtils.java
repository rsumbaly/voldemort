package voldemort.secondary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.TestUtils;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.Store;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/** Utility methods to test the secondary index support */
public class SecondaryIndexTestUtils {

    /** Provider for a byte array Store */
    public interface ByteArrayStoreProvider {

        Store<ByteArray, byte[], byte[]> getStore();
    }

    /** Test value {@link SerializerDefinition} */
    public static final SerializerDefinition VALUE_SERIALIZER_DEF = new SerializerDefinition("json",
                                                                                             "{\"data\":\"string\", \"status\":\"int8\", \"lastmod\":\"date\"}");

    /** Test value {@link Serializer} */
    @SuppressWarnings("unchecked")
    public static final Serializer<Object> VALUE_SERIALIZER = (Serializer<Object>) new DefaultSerializerFactory().getSerializer(VALUE_SERIALIZER_DEF);

    /** Test list of {@link SecondaryIndexDefinition} */
    public static final List<SecondaryIndexDefinition> SEC_IDX_DEFS = Arrays.asList(new SecondaryIndexDefinition("status",
                                                                                                                 "map",
                                                                                                                 "status",
                                                                                                                 "json",
                                                                                                                 "\"int8\""),
                                                                                    new SecondaryIndexDefinition("lastmod",
                                                                                                                 "map",
                                                                                                                 "lastmod",
                                                                                                                 "json",
                                                                                                                 "\"date\""));

    /**
     * Test {@link SecondaryIndexProcessor}, based on {@link #SEC_IDX_DEFS} and
     * {@value #VALUE_SERIALIZER_DEF}
     */
    public static final SecondaryIndexProcessor SEC_IDX_PROCESSOR = SecondaryIndexProcessorFactory.getProcessor(new DefaultSerializerFactory(),
                                                                                                                SEC_IDX_DEFS,
                                                                                                                VALUE_SERIALIZER_DEF);

    private final ByteArrayStoreProvider storeProvider;

    public SecondaryIndexTestUtils(ByteArrayStoreProvider storeProvider) {
        this.storeProvider = storeProvider;
    }

    private Store<ByteArray, byte[], byte[]> getStore() {
        return storeProvider.getStore();
    }

    public SecondaryIndexProcessor getSecIdxProcessor() {
        return SEC_IDX_PROCESSOR;
    }

    private ByteArray testKey(String key) {
        return new ByteArray(key.getBytes());
    }

    /** Creates a test value that complies with the serializer schema */
    public static Map<String, Object> testValue(String data, int status, Date lastMod) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("data", data);
        map.put("status", (byte) status);
        map.put("lastmod", lastMod);
        return map;
    }

    /**
     * Checks the given query return the given keys on the inner store. See
     * {@link Store#getKeysBySecondary(RangeQuery)}
     */
    public void assertQueryReturns(RangeQuery query, String... keys) {
        Set<String> got = Sets.newHashSet();
        for(ByteArray val: getStore().getKeysBySecondary(query)) {
            got.add(new String(val.get()));
        }
        Set<String> expected = Sets.newHashSet(Arrays.asList(keys));
        assertEquals(expected, got);
    }

    /** Creates a raw query (with serialized values) for the given range */
    public RangeQuery query(String field, Object from, Object to) {
        byte[] fromBytes = getSecIdxProcessor().serializeValue(field, from);
        byte[] toBytes = getSecIdxProcessor().serializeValue(field, to);
        return new RangeQuery(field, fromBytes, toBytes);
    }

    private byte[] testSerializedValue(String data, int status, Date lastMod) {
        return VALUE_SERIALIZER.toBytes(testValue(data, status, lastMod));
    }

    private Versioned<byte[]> putTestValue(String key, String data, int status, Date lastMod) {
        List<Version> versions = getStore().getVersions(testKey(key));
        Version newVer = versions.isEmpty() ? new VectorClock()
                                           : ((VectorClock) Iterables.getLast(versions)).incremented(0,
                                                                                                     System.currentTimeMillis());
        Versioned<byte[]> value = Versioned.value(testSerializedValue(data, status, lastMod),
                                                  newVer);
        getStore().put(testKey(key), value, null);
        return value;
    }

    private void removeKey(String... keys) {
        for(String key: keys) {
            List<Versioned<byte[]>> value = getStore().get(testKey(key), null);
            assertEquals(1, value.size());
            assertTrue(getStore().delete(testKey(key), value.get(0).getVersion()));
        }
    }

    /** Full fixture for secondary index testing, applied to the provided Store */
    public void testSecondaryIndex() throws Exception {
        putTestValue("k1", "myData1", 1, new Date(100));
        putTestValue("k2", "myData2", 2, new Date(101));
        putTestValue("k3", "myData3", 2, new Date(102));
        putTestValue("k4", "myData4", 1, new Date(103));

        putTestValue("k5", "myData5", 0, new Date(0));
        putTestValue("k6", "myData6", 6, new Date(150));
        putTestValue("k7", "myData7", 100, new Date(150000));

        assertQueryReturns(query("status", (byte) 1, (byte) 1), "k1", "k4");
        assertQueryReturns(query("status", (byte) 2, (byte) 2), "k2", "k3");
        assertQueryReturns(query("status", (byte) 1, (byte) 2), "k1", "k2", "k3", "k4");

        assertQueryReturns(query("lastmod", new Date(100), new Date(100)), "k1");
        assertQueryReturns(query("lastmod", new Date(101), new Date(102)), "k2", "k3");
        assertQueryReturns(query("lastmod", new Date(103), new Date(105)), "k4");
        assertQueryReturns(query("lastmod", new Date(90), new Date(110)), "k1", "k2", "k3", "k4");
        assertQueryReturns(query("lastmod", new Date(90), new Date(95)));
        assertQueryReturns(query("lastmod", new Date(104), new Date(105)));

        // update a value, and check it's properly reindexed
        putTestValue("k1", "myData1", 3, new Date(115));
        assertQueryReturns(query("lastmod", new Date(90), new Date(110)), "k2", "k3", "k4");
        assertQueryReturns(query("lastmod", new Date(110), new Date(140)), "k1");
        assertQueryReturns(query("status", (byte) 1, (byte) 2), "k2", "k3", "k4");
        assertQueryReturns(query("status", (byte) 3, (byte) 3), "k1");

        removeKey("k3");
        assertQueryReturns(query("status", (byte) 2, (byte) 2), "k2");
        assertQueryReturns(query("lastmod", new Date(101), new Date(105)), "k2", "k4");

        removeKey("k4");
        assertQueryReturns(query("status", (byte) 2, (byte) 2), "k2");
        assertQueryReturns(query("lastmod", new Date(101), new Date(105)), "k2");

        removeKey("k1", "k2");
        assertQueryReturns(query("status", (byte) 0, (byte) 10), "k5", "k6");
        assertQueryReturns(query("lastmod", new Date(0), new Date(200)), "k5", "k6");
    }

    public List<byte[]> getValues(int numValues) {
        List<byte[]> values = Lists.newArrayList();
        for(int i = 0; i < numValues; i++) {
            values.add(getValue(10));
        }
        return values;
    }

    public byte[] getValue(int size) {
        return testSerializedValue(new String(TestUtils.randomBytes(size)),
                                   (byte) TestUtils.SEEDED_RANDOM.nextInt() % 128,
                                   new Date(TestUtils.SEEDED_RANDOM.nextInt()));
    }

}

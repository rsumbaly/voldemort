package voldemort.store.memory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.secondary.RangeQuery;
import voldemort.secondary.SecondaryIndexProcessor;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An InMemoryStorageEngine extension that adds secondary index support.
 * 
 */
public class InMemoryStorageEngineSI extends InMemoryStorageEngine<ByteArray, byte[], byte[]> {

    // It would have been so great that TreeMultimap exposed the subMap method
    private final Map<String, NavigableMap<ByteArray, Set<ByteArray>>> secIndexes = Maps.newHashMap();
    private final SecondaryIndexProcessor secIdxProcessor;

    public InMemoryStorageEngineSI(String name) {
        this(name, new ConcurrentHashMap<ByteArray, List<Versioned<byte[]>>>(), null);
    }

    public InMemoryStorageEngineSI(String name, SecondaryIndexProcessor secIdxProcessor) {
        this(name, new ConcurrentHashMap<ByteArray, List<Versioned<byte[]>>>(), secIdxProcessor);
    }

    public InMemoryStorageEngineSI(String name,
                                   ConcurrentMap<ByteArray, List<Versioned<byte[]>>> map) {
        this(name, map, null);
    }

    public InMemoryStorageEngineSI(String name,
                                   ConcurrentMap<ByteArray, List<Versioned<byte[]>>> map,
                                   SecondaryIndexProcessor secIdxProcessor) {
        super(name, map);
        this.secIdxProcessor = secIdxProcessor;
        if(secIdxProcessor != null) {
            for(String field: secIdxProcessor.getSecondaryFields()) {
                // TODO synchronized?
                secIndexes.put(field, Maps.<ByteArray, Set<ByteArray>> newTreeMap());
            }
        }
    }

    @Override
    public void truncate() {
        super.truncate();
        for(NavigableMap<ByteArray, Set<ByteArray>> secIndex: secIndexes.values()) {
            secIndex.clear();
        }
    }

    @Override
    protected void putPostActions(ByteArray key,
                                  List<Versioned<byte[]>> deletedVals,
                                  List<Versioned<byte[]>> remainingVals,
                                  Versioned<byte[]> value,
                                  byte[] transforms) {
        if(secIdxProcessor == null)
            return;

        for(Versioned<byte[]> rawVal: deletedVals) {
            secIndexRemove(key, secIdxProcessor.extractSecondaryValues(rawVal.getValue()));
        }

        for(Versioned<byte[]> rawVal: remainingVals) {
            secIndexAdd(key, secIdxProcessor.extractSecondaryValues(rawVal.getValue()));
        }

        secIndexAdd(key, secIdxProcessor.extractSecondaryValues(value.getValue()));
    }

    @Override
    protected void deletePostActions(ByteArray key,
                                     List<Versioned<byte[]>> deletedVals,
                                     List<Versioned<byte[]>> remainingVals) {
        if(secIdxProcessor == null)
            return;

        for(Versioned<byte[]> rawVal: deletedVals) {
            secIndexRemove(key, secIdxProcessor.extractSecondaryValues(rawVal.getValue()));
        }

        for(Versioned<byte[]> rawVal: remainingVals) {
            secIndexAdd(key, secIdxProcessor.extractSecondaryValues(rawVal.getValue()));
        }
    }

    private void secIndexAdd(ByteArray key, Map<String, byte[]> values) {
        for(Entry<String, byte[]> entry: values.entrySet()) {
            String fieldName = entry.getKey();
            ByteArray secIdxKey = new ByteArray(entry.getValue());
            NavigableMap<ByteArray, Set<ByteArray>> secIndex = secIndexes.get(fieldName);

            Set<ByteArray> set = secIndex.get(secIdxKey);
            if(set == null) {
                set = Sets.newHashSet();
                secIndex.put(secIdxKey, set);
            }
            set.add(key);
        }
    }

    private void secIndexRemove(ByteArray key, Map<String, byte[]> values) {
        for(Entry<String, byte[]> entry: values.entrySet()) {
            String fieldName = entry.getKey();
            ByteArray secIdxKey = new ByteArray(entry.getValue());
            NavigableMap<ByteArray, Set<ByteArray>> secIndex = secIndexes.get(fieldName);

            Set<ByteArray> set = secIndex.get(secIdxKey);
            if(set != null) {
                set.remove(key);
                if(set.isEmpty())
                    secIndex.remove(secIdxKey);
            }
        }
    }

    @Override
    public Set<ByteArray> getKeysBySecondary(RangeQuery query) {
        NavigableMap<ByteArray, Set<ByteArray>> index = secIndexes.get(query.getField());

        ByteArray start = new ByteArray((byte[]) query.getStart());
        ByteArray end = new ByteArray((byte[]) query.getEnd());

        HashSet<ByteArray> result = Sets.newHashSet();
        for(Entry<ByteArray, Set<ByteArray>> entry: index.subMap(start, true, end, true).entrySet()) {
            result.addAll(entry.getValue());
        }
        return result;
    }
}

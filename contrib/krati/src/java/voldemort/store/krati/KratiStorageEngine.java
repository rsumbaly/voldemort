package voldemort.store.krati;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.WriteBufferSegmentFactory;
import krati.store.IndexedDataStore;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.StripedLock;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class KratiStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(KratiStorageEngine.class);
    private final String name;
    private final IndexedDataStore datastore;
    private final StripedLock locks;

    public KratiStorageEngine(String name,
                              int batchSize,
                              int indexSegmentFileSizeMB,
                              int storeSegmentFileSizeMB,
                              int lockStripes,
                              int initLevel,
                              File dataDirectory) {
        this.name = Utils.notNull(name);
        try {
            this.datastore = new IndexedDataStore(dataDirectory,
                                                  batchSize,
                                                  5,
                                                  initLevel,
                                                  indexSegmentFileSizeMB,
                                                  new MemorySegmentFactory(),
                                                  initLevel,
                                                  storeSegmentFileSizeMB,
                                                  new WriteBufferSegmentFactory(storeSegmentFileSizeMB));
            this.locks = new StripedLock(lockStripes);
        } catch(Exception e) {
            throw new VoldemortException("Failure initializing store.", e);
        }

    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public String getName() {
        return this.name;
    }

    public void close() throws VoldemortException {}

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key));
    }

    public void truncate() {
        try {
            datastore.clear();
        } catch(Exception e) {
            logger.error("Failed to truncate store '" + name + "': ", e);
            throw new VoldemortException("Failed to truncate store '" + name + "'.");
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        try {
            return disassembleValues(datastore.get(key.get()));
        } catch(Exception e) {
            logger.error("Error reading value: ", e);
            throw new VoldemortException("Error reading value: ", e);
        }
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new KratiEntriesIterator(datastore.iterator());
    }

    public ClosableIterator<ByteArray> keys() {
        return new KratiKeysIterator(datastore.keyIterator());
    }

    public boolean delete(ByteArray key, Version maxVersion) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        synchronized(this.locks.lockFor(key.get())) {
            if(maxVersion == null) {
                try {
                    return datastore.delete(key.get());
                } catch(Exception e) {
                    logger.error("Failed to delete key: ", e);
                    throw new VoldemortException("Failed to delete key: " + key, e);
                }
            }

            List<Versioned<byte[]>> returnedValuesList = this.get(key);

            // Case if there is nothing to delete
            if(returnedValuesList.size() == 0) {
                return false;
            }

            Iterator<Versioned<byte[]>> iter = returnedValuesList.iterator();
            while(iter.hasNext()) {
                Versioned<byte[]> currentValue = iter.next();
                Version currentVersion = currentValue.getVersion();
                if(currentVersion.compare(maxVersion) == Occured.BEFORE) {
                    iter.remove();
                }
            }

            try {
                if(returnedValuesList.size() == 0)
                    return datastore.delete(key.get());
                else
                    return datastore.put(key.get(), assembleValues(returnedValuesList));
            } catch(Exception e) {
                String message = "Failed to delete key " + key;
                logger.error(message, e);
                throw new VoldemortException(message, e);
            }
        }
    }

    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        synchronized(this.locks.lockFor(key.get())) {
            // First get the value
            List<Versioned<byte[]>> existingValuesList = this.get(key);

            // If no value, add one
            if(existingValuesList.size() == 0) {
                existingValuesList = new ArrayList<Versioned<byte[]>>();
                existingValuesList.add(new Versioned<byte[]>(value.getValue(), value.getVersion()));
            } else {

                // Update the value
                List<Versioned<byte[]>> removedValueList = new ArrayList<Versioned<byte[]>>();
                for(Versioned<byte[]> versioned: existingValuesList) {
                    Occured occured = value.getVersion().compare(versioned.getVersion());
                    if(occured == Occured.BEFORE)
                        throw new ObsoleteVersionException("Obsolete version for key '" + key
                                                           + "': " + value.getVersion());
                    else if(occured == Occured.AFTER)
                        removedValueList.add(versioned);
                }
                existingValuesList.removeAll(removedValueList);
                existingValuesList.add(value);
            }

            try {
                datastore.put(key.get(), assembleValues(existingValuesList));
            } catch(Exception e) {
                String message = "Failed to put key " + key;
                logger.error(message, e);
                throw new VoldemortException(message, e);
            }
        }
    }

    /**
     * Store the versioned values
     * 
     * @param values list of versioned bytes
     * @return the list of versioned values rolled into an array of bytes
     */
    private byte[] assembleValues(List<Versioned<byte[]>> values) throws IOException {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(stream);

        for(Versioned<byte[]> value: values) {
            byte[] object = value.getValue();
            dataStream.writeInt(object.length);
            dataStream.write(object);

            VectorClock clock = (VectorClock) value.getVersion();
            dataStream.writeInt(clock.sizeInBytes());
            dataStream.write(clock.toBytes());
        }

        return stream.toByteArray();
    }

    /**
     * Splits up value into multiple versioned values
     * 
     * @param value
     * @return
     * @throws IOException
     */
    private List<Versioned<byte[]>> disassembleValues(byte[] values) throws IOException {

        if(values == null)
            return new ArrayList<Versioned<byte[]>>(0);

        List<Versioned<byte[]>> returnList = new ArrayList<Versioned<byte[]>>();
        ByteArrayInputStream stream = new ByteArrayInputStream(values);
        DataInputStream dataStream = new DataInputStream(stream);

        while(dataStream.available() > 0) {
            byte[] object = new byte[dataStream.readInt()];
            dataStream.read(object);

            byte[] clockBytes = new byte[dataStream.readInt()];
            dataStream.read(clockBytes);
            VectorClock clock = new VectorClock(clockBytes);

            returnList.add(new Versioned<byte[]>(object, clock));
        }

        return returnList;
    }

    private class KratiEntriesIterator implements
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private Iterator<Entry<byte[], byte[]>> iterator;
        private ByteArray currentKey;
        private Iterator<Versioned<byte[]>> currentValues;

        public KratiEntriesIterator(Iterator<Entry<byte[], byte[]>> iterator) {
            this.iterator = iterator;
        }

        public void close() {
        // Nothing to close here
        }

        private boolean hasNextInCurrentValues() {
            return currentValues != null && currentValues.hasNext();
        }

        public boolean hasNext() {
            return hasNextInCurrentValues() || iterator.hasNext();
        }

        private Pair<ByteArray, Versioned<byte[]>> nextInCurrentValues() {
            Versioned<byte[]> item = currentValues.next();
            return Pair.create(currentKey, item);
        }

        public Pair<ByteArray, Versioned<byte[]>> next() {
            if(hasNextInCurrentValues()) {
                return nextInCurrentValues();
            } else {
                // keep trying to get a next, until we find one (they could get
                // removed)
                while(true) {
                    Entry<byte[], byte[]> entry = iterator.next();

                    List<Versioned<byte[]>> list;
                    try {
                        list = disassembleValues(entry.getValue());
                    } catch(IOException e) {
                        list = new ArrayList<Versioned<byte[]>>();
                    }
                    synchronized(list) {
                        // okay we may have gotten an empty list, if so try
                        // again
                        if(list.size() == 0)
                            continue;

                        // grab a snapshot of the list while we have exclusive
                        // access
                        currentValues = new ArrayList<Versioned<byte[]>>(list).iterator();
                    }
                    currentKey = new ByteArray(entry.getKey());
                    return nextInCurrentValues();
                }
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private class KratiKeysIterator implements ClosableIterator<ByteArray> {

        private Iterator<byte[]> iter;

        public KratiKeysIterator(Iterator<byte[]> iterator) {
            iter = iterator;
        }

        public void close() {
        // Nothing to close here
        }

        public boolean hasNext() {
            return iter.hasNext();
        }

        public ByteArray next() {
            return new ByteArray(iter.next());
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
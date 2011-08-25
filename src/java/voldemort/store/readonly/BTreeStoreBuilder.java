package voldemort.store.readonly;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;

import voldemort.VoldemortException;
import voldemort.store.readonly.JsonStoreBuilder.KeyValuePair;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;

import com.google.common.collect.Maps;

public class BTreeStoreBuilder {

    private long blockSize;
    private long maxLevel;
    private final String fileName;
    private final File parent;

    private HashMap<Long, DataOutputStream> indexStreams;
    private HashMap<Long, Long> indexBlockSizePerLevel;
    private HashMap<Long, Long> indexOffsetPerLevel;

    private long dataOffset;
    private DataOutputStream dataStream;

    public static class BTreeSearcher {

        private MessageDigest md5er;
        private final HashMap<Integer, MappedByteBuffer> levelToIndexFileBuffers;
        private final HashMap<Integer, Long> levelToIndexFileSizes;
        private final FileChannel dataFile;
        private int levels;
        private long sizeOfBlock;

        public BTreeSearcher(File parent, String fileName, long blockSize) throws Exception {
            this.md5er = MessageDigest.getInstance("md5");
            this.sizeOfBlock = blockSize;

            // Load the data file
            File file = new File(parent, fileName + ".data");
            if(!file.exists()) {
                throw new Exception("Data file does not exist");
            }
            this.dataFile = new FileInputStream(file).getChannel();

            // Open the index files
            this.levelToIndexFileSizes = Maps.newHashMap();
            this.levelToIndexFileBuffers = Maps.newHashMap();
            levels = 0;
            while(true) {
                file = new File(parent, fileName + ".index." + levels);
                if(!file.exists()) {
                    if(levels == 0) {
                        throw new Exception("Could not find any index file");
                    } else {
                        break;
                    }
                }
                this.levelToIndexFileSizes.put(levels, file.length());
                this.levelToIndexFileBuffers.put(levels,
                                                 new FileInputStream(file).getChannel()
                                                                          .map(MapMode.READ_ONLY,
                                                                               0,
                                                                               file.length()));
                levels++;
            }
        }

        public byte[] getValue(byte[] key) throws IOException {
            try {
                int searchLevel = levels;
                byte[] keyMD5 = md5er.digest(key);
                int startOffset = 0;
                while(searchLevel > 0) {
                    long indexFileSize = levelToIndexFileSizes.get(searchLevel - 1);
                    startOffset = indexOf(levelToIndexFileBuffers.get(searchLevel - 1),
                                          keyMD5,
                                          startOffset,
                                          (int) indexFileSize,
                                          (searchLevel - 1 == 0) ? true : false);
                    System.out.println("Offset - " + startOffset);
                    searchLevel--;
                }

                // Read value size
                ByteBuffer sizeBuffer = ByteBuffer.allocate(ByteUtils.SIZE_OF_INT);
                dataFile.read(sizeBuffer, startOffset);
                int valueSize = sizeBuffer.getInt(0);

                // Read value
                ByteBuffer valueBuffer = ByteBuffer.allocate(valueSize);
                dataFile.read(valueBuffer, startOffset + ByteUtils.SIZE_OF_INT);
                return valueBuffer.array();
            } finally {
                md5er.reset();
            }
        }

        public int indexOf(ByteBuffer index,
                           byte[] key,
                           int startOffset,
                           int indexFileSize,
                           boolean lastLevel) {
            byte[] keyBuffer = new byte[key.length];
            int indexSize = ReadOnlyUtils.POSITION_SIZE + key.length;
            int low = startOffset / indexSize;
            int high = Math.min(indexFileSize / indexSize, low + (int) sizeOfBlock) - 1;
            int returnOffset = -1;
            while(low <= high) {
                int mid = (low + high) / 2;
                index.position(mid * indexSize);
                index.get(keyBuffer);
                index.position(mid * indexSize + key.length);
                returnOffset = index.getInt();
                int cmp = ByteUtils.compare(keyBuffer, key);
                if(cmp == 0) {
                    // they are equal, return the location stored here
                    return returnOffset;
                } else if(cmp > 0) {
                    // midVal is bigger
                    high = mid - 1;
                    returnOffset -= (sizeOfBlock * indexSize);
                } else if(cmp < 0) {
                    // the keyMd5 is bigger
                    low = mid + 1;
                }
            }
            if(lastLevel) {
                return -1;
            } else {
                return returnOffset;
            }
        }

    }

    public BTreeStoreBuilder(File parent, String fileName, long blockSize)
                                                                          throws FileNotFoundException {
        this.parent = parent;
        this.fileName = fileName;
        this.blockSize = blockSize;
        this.maxLevel = 0;
        this.indexStreams = Maps.newHashMap();
        this.indexStreams.put(maxLevel,
                              new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(parent,
                                                                                                          fileName
                                                                                                                  + ".index."
                                                                                                                  + maxLevel)))));

        this.indexOffsetPerLevel = Maps.newHashMap();
        this.indexOffsetPerLevel.put(0L, 0L);

        this.indexBlockSizePerLevel = Maps.newHashMap();
        this.indexBlockSizePerLevel.put(0L, 0L);

        this.dataStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(parent,
                                                                                                      fileName
                                                                                                              + ".data"))));
        this.dataOffset = 0L;
    }

    /**
     * In sorted md5 key order
     * 
     * @throws IOException
     */
    void build(Iterable<KeyValuePair> iterable) throws IOException {
        build(iterable.iterator());
    }

    /**
     * In sorted md5 key order
     * 
     * @throws IOException
     */
    void build(Iterator<KeyValuePair> iterator) throws IOException {
        try {
            while(iterator.hasNext()) {
                KeyValuePair pair = iterator.next();

                // Go over every level and make some decisions
                long currentLevel = 0;
                boolean updateForNextLevel = false;
                long previousLevelOffset = dataOffset;
                do {
                    DataOutputStream stream = this.indexStreams.get(currentLevel);
                    stream.write(pair.getKeyMd5());
                    stream.writeInt((int) previousLevelOffset);

                    // Bump up the group size
                    Long currentBlockSize = indexBlockSizePerLevel.get(currentLevel);
                    currentBlockSize++;
                    indexBlockSizePerLevel.put(currentLevel, currentBlockSize);

                    // Increment the offset
                    Long currentOffset = indexOffsetPerLevel.get(currentLevel);
                    previousLevelOffset = currentOffset.longValue();
                    currentOffset += (16 + ByteUtils.SIZE_OF_INT);
                    indexOffsetPerLevel.put(currentLevel, currentOffset);

                    if(currentBlockSize > blockSize) {
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
                            indexStreams.put(currentLevel,
                                             new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(parent,
                                                                                                                         fileName
                                                                                                                                 + ".index."
                                                                                                                                 + currentLevel)))));
                        } else {
                            currentLevel++;
                        }
                    } else {
                        updateForNextLevel = false;
                    }
                } while(currentLevel <= maxLevel && updateForNextLevel);

                // Write to data stream
                this.dataStream.writeInt(pair.getValue().length);
                this.dataStream.write(pair.getValue());
                this.dataOffset += (ByteUtils.SIZE_OF_INT + pair.getValue().length);
            }
        } finally {

            // Close the data stream
            if(this.dataStream != null) {
                this.dataStream.flush();
                this.dataStream.close();
            }

            // Close the index streams
            for(DataOutputStream stream: this.indexStreams.values()) {
                stream.flush();
                stream.close();
            }
        }
    }

    public static void main(String args[]) throws Exception {
        if(args.length != 3) {
            System.err.println("[source file] [destinationdirectory] [block size]");
            System.exit(1);
        }

        String sourceFilePath = args[0];
        String destinationDirectoryPath = args[1];
        String blockSizeString = args[2];

        if(!Utils.isReadableFile(sourceFilePath) || !Utils.isReadableDir(destinationDirectoryPath)) {
            System.err.println("Directory does not exist or cannot be read");
            System.exit(1);
        }
        File sourceDirectory = new File(sourceFilePath);
        File destinationDirectory = new File(destinationDirectoryPath);
        long blockSize = Long.parseLong(blockSizeString);
        BTreeStoreBuilder builder = new BTreeStoreBuilder(destinationDirectory, "blah", blockSize);

        final DataInputStream reader = new DataInputStream(new FileInputStream(sourceDirectory));
        final MessageDigest digest = MessageDigest.getInstance("md5");
        builder.build(new Iterator<KeyValuePair>() {

            public boolean hasNext() {
                try {
                    return reader.available() > 0;
                } catch(IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }

            public KeyValuePair next() {
                try {
                    String line;
                    try {
                        line = reader.readLine();
                    } catch(IOException e) {
                        return null;
                    }
                    if(line != null) {
                        String[] tuples = line.split("\t");
                        byte[] keyBytes = tuples[0].getBytes();
                        byte[] valueBytes = tuples[1].getBytes();
                        return new KeyValuePair(keyBytes, digest.digest(keyBytes), valueBytes);
                    }
                    return null;
                } finally {
                    digest.reset();
                }
            }

            public void remove() {
                throw new VoldemortException("Do not support remove");
            }
        });

    }

    // public static void main(String args[]) throws Exception {
    // if(args.length != 2) {
    // System.err.println("[root directory] [block size]");
    // System.exit(1);
    // }
    // String rootDirectoryPath = args[1];
    // if(!Utils.isReadableDir(rootDirectoryPath)) {
    // System.err.println("Directory does not exist or cannot be read");
    // System.exit(1);
    // }
    // File rootDirectory = new File(rootDirectoryPath);
    // long blockSize = Long.parseLong(args[2]);
    // BTreeStoreBuilder builder = new BTreeStoreBuilder(rootDirectory, "blah",
    // blockSize);
    //
    // List<KeyValuePair> tuples = Lists.newArrayList();
    // CheckSum checksum = CheckSum.getInstance(CheckSumType.MD5);
    // for(int i = 0; i < 100000; i++) {
    // byte[] key = new byte[ByteUtils.SIZE_OF_INT];
    // ByteUtils.writeInt(key, i, 0);
    // byte[] md5key = checksum.updateAndGetCheckSum(key, 0, key.length);
    // byte[] value = TestUtils.randomBytes(100);
    // tuples.add(new KeyValuePair(key, md5key, value));
    // }
    //
    // ExternalSorter<KeyValuePair> sorter = new
    // ExternalSorter<KeyValuePair>(new KeyValuePairSerializer(),
    // new KeyMd5Comparator(),
    // 1000,
    // TestUtils.createTempDir()
    // .getAbsolutePath(),
    // 1000000,
    // 10,
    // false);
    // builder.build(sorter.sorted(tuples.iterator()));
    //
    // BTreeSearcher searcher = new BTreeSearcher(rootDirectory, "blah",
    // blockSize);
    // for(KeyValuePair tuple: tuples) {
    // System.out.println("Search - " + ByteUtils.readInt(tuple.getKey(), 0) +
    // " - "
    // + ByteUtils.toHexString(tuple.getKeyMd5()));
    // Assert.assertEquals(ByteUtils.compare(tuple.getValue(),
    // searcher.getValue(tuple.getKey())), 0);
    // }
    // }
}

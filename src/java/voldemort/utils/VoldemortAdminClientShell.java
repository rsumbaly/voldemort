package voldemort.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.VoldemortClientShell;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerFactory;
import voldemort.serialization.json.EndOfFileException;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Toy shell for voldemort admin client
 * 
 */
public class VoldemortAdminClientShell extends VoldemortClientShell {

    private static final String PROMPT = "> ";

    public static void main(String[] args) throws Exception {
        if(args.length < 1 || args.length > 2)
            Utils.croak("USAGE: java VoldemortAdminClientShell bootstrap_url [command_file]");

        String bootstrapUrl = args[0];
        String commandsFileName = "";
        BufferedReader fileReader = null;
        BufferedReader inputReader = null;
        try {
            if(args.length == 2) {
                commandsFileName = args[1];
                fileReader = new BufferedReader(new FileReader(commandsFileName));
            }
            inputReader = new BufferedReader(new InputStreamReader(System.in));
        } catch(IOException e) {
            Utils.croak("Failure to open input stream: " + e.getMessage());
        }

        AdminClient adminClient = null;
        try {
            adminClient = new AdminClient(bootstrapUrl, new AdminClientConfig());
        } catch(Exception e) {
            Utils.croak("Couldn't instantiate admin client: " + e.getMessage());
        }

        System.out.println("Created admin client to cluster at " + bootstrapUrl);
        System.out.print(PROMPT);
        if(fileReader != null) {
            processCommands(fileReader, adminClient, true);
            fileReader.close();
        }
        processCommands(inputReader, adminClient, false);

    }

    public static void processCommands(BufferedReader reader,
                                       AdminClient adminClient,
                                       boolean printCommands) throws IOException {
        for(String line = reader.readLine(); line != null; line = reader.readLine()) {
            if(line.trim().equals(""))
                continue;
            if(printCommands)
                System.out.println(line);
            try {
                if(line.toLowerCase().startsWith("getmetadata")) {

                    String[] args = line.substring("getmetadata".length() + 1).split("\\s+");

                    // Parse the parameters
                    String metadataKey = args[0];
                    int remoteNodeId = -1;
                    if(args.length == 2)
                        remoteNodeId = Integer.valueOf(args[1]);

                    executeGetMetadata(remoteNodeId, adminClient, metadataKey);

                } else if(line.toLowerCase().startsWith("delete-partitions")) {
                    String[] args = line.substring("delete-partitions".length() + 1).split("\\s+");

                    List<String> storeNames = parseStoreNames(args[0]);
                    List<Integer> partitionList = parseCsv(args[1]);
                    int nodeId = Integer.valueOf(args[2]);

                    executeDeletePartitions(nodeId, adminClient, partitionList, storeNames);

                } else if(line.toLowerCase().startsWith("restore")) {

                    String[] args = line.substring("restore".length() + 1).split("\\s+");

                    int nodeId = Integer.valueOf(args[0]);
                    int parallelism = Integer.valueOf(args[1]);

                    System.out.println("Starting restore");
                    adminClient.restoreDataFromReplications(nodeId, parallelism);
                    System.out.println("Finished restore");

                } else if(line.toLowerCase().startsWith("add-stores")) {

                    String[] args = line.substring("add-stores".length() + 1).split("\\s+");

                    File storesFile = new File(args[0]);
                    if(!Utils.isReadableFile(storesFile)) {
                        System.err.println("Cannot open stores xml");
                        return;
                    }

                    executeAddStores(adminClient, storesFile);

                } else if(line.toLowerCase().startsWith("delete-stores")) {

                    String[] args = line.substring("delete-stores".length() + 1).split("\\s+");

                    List<String> storeNames = parseStoreNames(args[0]);

                    executeDeleteStores(adminClient, storeNames);

                } else if(line.toLowerCase().startsWith("truncate-stores")) {

                    String[] args = line.substring("truncate-stores".length() + 1).split("\\s+");

                    List<String> storeNames = parseStoreNames(args[0]);

                    executeTruncateStores(adminClient, storeNames);

                } else if(line.toLowerCase().startsWith("ro-version")) {

                    String[] args = line.substring("ro-version".length() + 1).split("\\s+");

                    List<String> storeNames = parseStoreNames(args[0]);
                    String versionType = args[1];
                    int nodeId = -1;
                    if(args.length == 3) {
                        nodeId = Integer.parseInt(args[2]);
                    }

                    executeROVersion(nodeId, adminClient, storeNames, versionType);

                } else if(line.toLowerCase().startsWith("set-metadata")) {

                    String[] args = line.substring("set-metadata".length() + 1).split("\\s+");

                    String metadataKey = args[0];
                    String metadataValue = args[1];
                    int nodeId = -1;
                    if(args.length == 3) {
                        nodeId = Integer.parseInt(args[2]);
                    }

                    if(metadataKey.compareTo(MetadataStore.CLUSTER_KEY) == 0) {
                        if(!Utils.isReadableFile(metadataValue)) {
                            System.err.println("Cluster xml file path incorrect");
                            return;
                        }
                        ClusterMapper mapper = new ClusterMapper();
                        Cluster newCluster = mapper.readCluster(new File(metadataValue));
                        executeSetMetadata(nodeId,
                                           adminClient,
                                           MetadataStore.CLUSTER_KEY,
                                           mapper.writeCluster(newCluster));
                    } else if(metadataKey.compareTo(MetadataStore.SERVER_STATE_KEY) == 0) {
                        VoldemortState newState = VoldemortState.valueOf(metadataValue);
                        executeSetMetadata(nodeId,
                                           adminClient,
                                           MetadataStore.SERVER_STATE_KEY,
                                           newState.toString());
                    } else if(metadataKey.compareTo(MetadataStore.STORES_KEY) == 0) {
                        if(!Utils.isReadableFile(metadataValue)) {
                            System.err.println("Stores definition xml file path incorrect");
                            return;
                        }
                        StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
                        List<StoreDefinition> storeDefs = mapper.readStoreList(new File(metadataValue));
                        executeSetMetadata(nodeId,
                                           adminClient,
                                           MetadataStore.STORES_KEY,
                                           mapper.writeStoreList(storeDefs));
                    } else {
                        System.err.println("Incorrect metadata key");
                    }
                } else if(line.toLowerCase().startsWith("fetchkeys")) {
                    String[] args = line.substring("fetchkeys".length() + 1).split("\\s+");

                    // Parse the parameters
                    List<String> storeNames = parseStoreNames(args[0]);
                    List<Integer> partitionList = parseCsv(args[1]);
                    int remoteNodeId = Integer.valueOf(args[2]);

                    File dirFile = null;
                    boolean useBoolean = false;

                    if(args.length > 3) {
                        if(args[3].contains("use_binary")) {
                            useBoolean = true;
                        } else {
                            dirFile = new File(args[3]);
                        }

                        if(args.length == 5) {
                            if(args[4].contains("use_binary")) {
                                useBoolean = true;
                            } else {
                                dirFile = new File(args[4]);
                            }
                        }
                    }
                    executeFetchKeys(remoteNodeId,
                                     adminClient,
                                     partitionList,
                                     dirFile,
                                     storeNames,
                                     useBoolean);

                } else if(line.toLowerCase().startsWith("fetch")) {
                    String[] args = line.substring("fetch".length() + 1).split("\\s+");

                    List<String> storeNames = parseStoreNames(args[0]);
                    List<Integer> partitionList = parseCsv(args[1]);
                    int remoteNodeId = Integer.valueOf(args[2]);

                    File dirFile = null;
                    boolean useBoolean = false;

                    if(args.length > 3) {
                        if(args[3].contains("use_binary")) {
                            useBoolean = true;
                        } else {
                            dirFile = new File(args[3]);
                        }

                        if(args.length == 5) {
                            if(args[4].contains("use_binary")) {
                                useBoolean = true;
                            } else {
                                dirFile = new File(args[4]);
                            }
                        }
                    }

                    executeFetchEntries(remoteNodeId,
                                        adminClient,
                                        partitionList,
                                        dirFile,
                                        storeNames,
                                        useBoolean);

                } else if(line.toLowerCase().startsWith("check-consistent")) {

                    String[] args = line.substring("check-consistent".length() + 1).split("\\s+");

                    String key = args[0];

                    executeCheckConsistent(adminClient, key);

                } else if(line.startsWith("help")) {
                    System.out.println("Commands:");
                    System.out.println("getmetadata key <node_id> -- Get metadata associated for key "
                                       + "[stores.xml | cluster.xml | server.state]");
                    System.out.println("fetchkeys store_name partitions node_id [folder_name | use_binary] -- Fetch all keys from given partitions"
                                       + " (a comma separated list) of store_name on node_id. Optionally, write to a folder.");
                    System.out.println("fetch store_names partitions node_id [folder_name | use_binary] -- Fetch all entries from given partitions"
                                       + " (a comma separated list) of store_name on node_id. Optionally, write to a folder.");
                    System.out.println("delete-partitions store_names partitions node_id -- Deletes the entries of partitions on the stores mentioned");
                    System.out.println("restore node-id parallelism -- Restore the data on node id with a preset parallelism ");
                    System.out.println("add-stores store-definition-xml -- Adds the store to the cluster");
                    System.out.println("delete-stores store_names -- Delete all the stores (comma separated list)");
                    System.out.println("truncate-store store_names -- Delete the data on all stores (comma separated list)");
                    System.out.println("ro-version store_names <max | current> <node_id> -- Returns the RO version [max | current] for store names");
                    System.out.println("set-metadata key value -- Set the remote metadata key [cluster.xml | stores.xml | server.state]");
                    System.out.println("check-consistent key -- Retrieves metadata key from all nodes and checks if they are consistent [cluster.xml | stores.xml]");
                    System.out.println("help -- Print this message.");
                    System.out.println("exit -- Exit from this shell.");
                    System.out.println();
                } else if(line.startsWith("quit") || line.startsWith("exit")) {
                    System.out.println("k k thx bye.");
                    System.exit(0);
                } else {
                    System.err.println("Invalid command.");
                }
            } catch(EndOfFileException e) {
                System.err.println("Expected additional token");
            } catch(VoldemortException e) {
                System.err.println("Exception thrown during operation.");
                e.printStackTrace();
            } catch(ArrayIndexOutOfBoundsException e) {
                System.err.println("Invalid command.");
            } catch(Exception e) {
                System.err.println("Unexpected error:");
                e.printStackTrace(System.err);
            }
            System.out.print(PROMPT);
        }
    }

    public static void executeCheckConsistent(AdminClient adminClient, String key) {
        List<Integer> nodeIds = Lists.newArrayList();
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            nodeIds.add(node.getId());
        }

        if(key.compareTo(MetadataStore.CLUSTER_KEY) == 0) {
            Set<Cluster> clusters = Sets.newHashSet();
            ClusterMapper mapper = new ClusterMapper();

            for(Integer currentNodeId: nodeIds) {
                Versioned<String> versioned = adminClient.getRemoteMetadata(currentNodeId, key);
                if(versioned == null) {
                    System.err.println("Metadata for key " + key + "returned null");
                    return;
                }
                clusters.add(mapper.readCluster(new StringReader(versioned.getValue())));
            }
            if(clusters.size() == 1)
                System.out.println("true");
            else
                System.out.println("false");

        } else if(key.compareTo(MetadataStore.STORES_KEY) == 0) {
            StoreDefinitionsMapper mapper = new StoreDefinitionsMapper();
            Set<Set<StoreDefinition>> storeDefs = Sets.newHashSet();

            for(Integer currentNodeId: nodeIds) {
                Versioned<String> versioned = adminClient.getRemoteMetadata(currentNodeId, key);
                if(versioned == null) {
                    System.err.println("Metadata for key " + key + "returned null");
                    return;
                }
                storeDefs.add(Sets.newHashSet(mapper.readStoreList(new StringReader(versioned.getValue()))));
            }
            if(storeDefs.size() == 1)
                System.out.println("true");
            else
                System.out.println("false");
        } else {
            System.err.println("Key type " + key + " is not supported");
        }

    }

    private static List<Integer> parseCsv(String csv) {
        return Lists.transform(Arrays.asList(csv.split(",")), new Function<String, Integer>() {

            public Integer apply(String input) {
                return Integer.valueOf(input.trim());
            }
        });
    }

    private static List<String> parseStoreNames(String csv) {
        return Lists.transform(Arrays.asList(csv.split(",")), new Function<String, String>() {

            public String apply(String input) {
                return input.trim();
            }
        });
    }

    public static void executeSetMetadata(Integer nodeId,
                                          AdminClient adminClient,
                                          String key,
                                          Object value) {

        List<Integer> nodeIds = Lists.newArrayList();
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                nodeIds.add(node.getId());
            }
        } else {
            nodeIds.add(nodeId);
        }
        for(Integer currentNodeId: nodeIds) {
            System.out.println("Setting "
                               + key
                               + " for "
                               + adminClient.getAdminClientCluster()
                                            .getNodeById(currentNodeId)
                                            .getHost()
                               + ":"
                               + adminClient.getAdminClientCluster()
                                            .getNodeById(currentNodeId)
                                            .getId());
            Versioned<String> currentValue = adminClient.getRemoteMetadata(currentNodeId, key);
            if(!value.equals(currentValue.getValue())) {
                VectorClock updatedVersion = ((VectorClock) currentValue.getVersion()).incremented(currentNodeId,
                                                                                                   System.currentTimeMillis());
                adminClient.updateRemoteMetadata(currentNodeId,
                                                 key,
                                                 Versioned.value(value.toString(), updatedVersion));
            }
        }
    }

    public static void executeROVersion(Integer nodeId,
                                        AdminClient adminClient,
                                        List<String> storeNames,
                                        String versionType) {
        Map<String, Long> storeToVersion = Maps.newHashMap();

        if(storeNames == null) {
            // Retrieve list of read-only stores
            storeNames = Lists.newArrayList();
            for(StoreDefinition storeDef: adminClient.getRemoteStoreDefList(nodeId > 0 ? nodeId : 0)
                                                     .getValue()) {
                if(storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {
                    storeNames.add(storeDef.getName());
                }
            }
        }

        if(nodeId < 0) {
            if(versionType.compareTo("max") != 0) {
                System.err.println("Unsupported operation, only max allowed for all nodes");
                return;
            }
            storeToVersion = adminClient.getROMaxVersion(storeNames);
        } else {
            if(versionType.compareTo("max") == 0) {
                storeToVersion = adminClient.getROMaxVersion(nodeId, storeNames);
            } else if(versionType.compareTo("current") == 0) {
                storeToVersion = adminClient.getROCurrentVersion(nodeId, storeNames);
            } else {
                System.err.println("Unsupported operation, only max OR current allowed for individual nodes");
                return;
            }
        }

        for(String storeName: storeToVersion.keySet()) {
            System.out.println(storeName + ":" + storeToVersion.get(storeName));
        }
    }

    public static void executeGetMetadata(Integer nodeId,
                                          AdminClient adminClient,
                                          String metadataKey) {
        List<Integer> nodeIds = Lists.newArrayList();
        if(nodeId < 0) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                nodeIds.add(node.getId());
            }
        } else {
            nodeIds.add(nodeId);
        }
        for(Integer currentNodeId: nodeIds) {
            System.out.println(adminClient.getAdminClientCluster()
                                          .getNodeById(currentNodeId)
                                          .getHost()
                               + ":"
                               + adminClient.getAdminClientCluster()
                                            .getNodeById(currentNodeId)
                                            .getId());
            Versioned<String> versioned = adminClient.getRemoteMetadata(currentNodeId, metadataKey);
            if(versioned == null) {
                System.out.println("null");
            } else {
                System.out.println(versioned.getVersion());
                System.out.print(": ");
                System.out.println(versioned.getValue());
                System.out.println();
            }
        }
    }

    public static void executeDeleteStores(AdminClient adminClient, List<String> storeNames) {
        for(String storeName: storeNames) {
            System.out.println("Deleting " + storeName + " on all nodes");
            adminClient.deleteStore(storeName);
        }
    }

    public static void executeTruncateStores(AdminClient adminClient, List<String> storeNames) {
        for(String storeName: storeNames) {
            for(Node node: adminClient.getAdminClientCluster().getNodes()) {
                System.out.println("Truncating " + storeName + " on node " + node.getId());
                adminClient.truncate(node.getId(), storeName);
            }
        }
    }

    public static void executeAddStores(AdminClient adminClient, File storesFile)
            throws IOException {
        List<StoreDefinition> storeDefinitionList = new StoreDefinitionsMapper().readStoreList(storesFile);
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for(StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }
        for(String store: storeDefinitionMap.keySet()) {
            System.out.println("Adding " + store);
            adminClient.addStore(storeDefinitionMap.get(store));
        }
    }

    public static void executeFetchEntries(Integer nodeId,
                                           AdminClient adminClient,
                                           List<Integer> partitionIdList,
                                           File file,
                                           List<String> storeNames,
                                           boolean useBoolean) throws IOException {

        List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId)
                                                               .getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for(StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }

        for(String store: storeNames) {
            StoreDefinition storeDefinition = storeDefinitionMap.get(store);
            if(storeDefinition != null) {
                System.out.println("Fetching entries in partitions "
                                   + Joiner.on(", ").join(partitionIdList) + " of " + store);
                Iterator<Pair<ByteArray, Versioned<byte[]>>> entriesIterator = adminClient.fetchEntries(nodeId,
                                                                                                        store,
                                                                                                        partitionIdList,
                                                                                                        null,
                                                                                                        false);

                if(useBoolean) {
                    writeEntriesBinary(entriesIterator, file, storeDefinition);
                } else {
                    writeEntriesAscii(entriesIterator, file, storeDefinition);
                }
            }
        }
    }

    private static void writeEntriesAscii(Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator,
                                          File dirFile,
                                          StoreDefinition storeDefinition) throws IOException {

        OutputStream writer = System.out;
        if(dirFile != null) {
            if(!Utils.isReadableDir(dirFile)) {
                System.err.println("Cannot access folder " + dirFile.getAbsolutePath());
                throw new VoldemortException("Cannot access folder");
            }
            writer = new FileOutputStream(new File(dirFile, storeDefinition.getName()));
        }

        SerializerFactory serializerFactory = new DefaultSerializerFactory();
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

        @SuppressWarnings("unchecked")
        Serializer<Object> keySerializer = (Serializer<Object>) serializerFactory.getSerializer(storeDefinition.getKeySerializer());
        @SuppressWarnings("unchecked")
        Serializer<Object> valueSerializer = (Serializer<Object>) serializerFactory.getSerializer(storeDefinition.getValueSerializer());

        try {
            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> kvPair = iterator.next();
                byte[] keyBytes = kvPair.getFirst().get();
                VectorClock version = (VectorClock) kvPair.getSecond().getVersion();
                byte[] valueBytes = kvPair.getSecond().getValue();

                Object keyObject = keySerializer.toObject(keyBytes);
                Object valueObject = valueSerializer.toObject(valueBytes);

                generator.writeObject(keyObject);
                stringWriter.write(' ');
                stringWriter.write(version.toString());
                generator.writeObject(valueObject);

                StringBuffer buf = stringWriter.getBuffer();
                if(buf.charAt(0) == ' ') {
                    buf.setCharAt(0, '\n');
                }
                writer.write(buf.toString().getBytes());
                buf.setLength(0);
            }
            writer.write('\n');
        } finally {
            if(dirFile != null)
                writer.close();
        }
    }

    private static void writeEntriesBinary(Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator,
                                           File dirFile,
                                           StoreDefinition storeDefinition) throws IOException {
        OutputStream writer = System.out;
        if(dirFile != null) {
            if(!Utils.isReadableDir(dirFile)) {
                System.err.println("Cannot access folder " + dirFile.getAbsolutePath());
                throw new VoldemortException("Cannot access folder");
            }
            writer = new FileOutputStream(new File(dirFile, storeDefinition.getName()));
        }

        try {
            while(iterator.hasNext()) {
                Pair<ByteArray, Versioned<byte[]>> kvPair = iterator.next();
                byte[] keyBytes = kvPair.getFirst().get();
                byte[] versionBytes = ((VectorClock) kvPair.getSecond().getVersion()).toBytes();
                byte[] valueBytes = kvPair.getSecond().getValue();
                writer.write(keyBytes.length);
                writer.write(keyBytes);
                writer.write(versionBytes.length);
                writer.write(versionBytes);
                writer.write(valueBytes.length);
                writer.write(valueBytes);
            }
        } finally {
            if(dirFile != null)
                writer.close();
        }
    }

    public static void executeFetchKeys(Integer nodeId,
                                        AdminClient adminClient,
                                        List<Integer> partitionIdList,
                                        File dirFile,
                                        List<String> storeNames,
                                        boolean useBoolean) throws IOException {

        List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId)
                                                               .getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for(StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }

        for(String store: storeNames) {
            StoreDefinition storeDefinition = storeDefinitionMap.get(store);
            if(storeDefinition != null) {
                System.out.println("Fetching keys in partitions "
                                   + Joiner.on(", ").join(partitionIdList) + " of " + store);
                Iterator<ByteArray> keyIterator = adminClient.fetchKeys(nodeId,
                                                                        store,
                                                                        partitionIdList,
                                                                        null,
                                                                        false);

                if(useBoolean) {
                    writeKeysBinary(keyIterator, dirFile, storeDefinition);
                } else {
                    writeKeysAscii(keyIterator, dirFile, storeDefinition);
                }
            }
        }

    }

    private static void writeKeysAscii(Iterator<ByteArray> keyIterator,
                                       File dirFile,
                                       StoreDefinition storeDefinition) throws IOException {

        OutputStream writer = System.out;
        if(dirFile != null) {
            if(!Utils.isReadableDir(dirFile)) {
                System.err.println("Cannot access folder " + dirFile.getAbsolutePath());
                throw new VoldemortException("Cannot access folder");
            }
            writer = new FileOutputStream(new File(dirFile, storeDefinition.getName()));
        }

        SerializerFactory serializerFactory = new DefaultSerializerFactory();
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);
        @SuppressWarnings("unchecked")
        Serializer<Object> serializer = (Serializer<Object>) serializerFactory.getSerializer(storeDefinition.getKeySerializer());
        try {
            while(keyIterator.hasNext()) {
                byte[] keyBytes = keyIterator.next().get();
                Object keyObject = serializer.toObject(keyBytes);
                generator.writeObject(keyObject);
                StringBuffer buf = stringWriter.getBuffer();
                if(buf.charAt(0) == ' ') {
                    buf.setCharAt(0, '\n');
                }
                writer.write(buf.toString().getBytes());
                buf.setLength(0);
            }
            writer.write('\n');
        } finally {
            if(dirFile != null)
                writer.close();
        }
    }

    private static void writeKeysBinary(Iterator<ByteArray> keyIterator,
                                        File dirFile,
                                        StoreDefinition storeDefinition) throws IOException {
        OutputStream writer = System.out;
        if(dirFile != null) {
            if(!Utils.isReadableDir(dirFile)) {
                System.err.println("Cannot access folder " + dirFile.getAbsolutePath());
                throw new VoldemortException("Cannot access folder");
            }
            writer = new FileOutputStream(new File(dirFile, storeDefinition.getName()));
        }

        try {
            while(keyIterator.hasNext()) {
                byte[] keyBytes = keyIterator.next().get();
                writer.write(keyBytes.length);
                writer.write(keyBytes);
            }
        } finally {
            if(dirFile != null)
                writer.close();
        }
    }

    public static void executeDeletePartitions(Integer nodeId,
                                               AdminClient adminClient,
                                               List<Integer> partitionIdList,
                                               List<String> storeNames) {
        List<String> stores = storeNames;
        if(stores == null) {
            stores = Lists.newArrayList();
            List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId)
                                                                   .getValue();
            for(StoreDefinition storeDefinition: storeDefinitionList) {
                stores.add(storeDefinition.getName());
            }
        }

        for(String store: stores) {
            System.out.println("Deleting partitions " + Joiner.on(", ").join(partitionIdList)
                               + " of " + store);
            adminClient.deletePartitions(nodeId, store, partitionIdList, null);
        }
    }
}

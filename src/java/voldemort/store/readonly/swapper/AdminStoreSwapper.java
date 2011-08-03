package voldemort.store.readonly.swapper;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.readonly.ReadOnlyUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AdminStoreSwapper extends StoreSwapper {

    private static final Logger logger = Logger.getLogger(AdminStoreSwapper.class);

    private AdminClient adminClient;
    private long timeoutMs;
    private boolean deleteFailedFetch = false;
    private boolean rollbackFailedSwap = false;

    /**
     * 
     * @param cluster The cluster metadata
     * @param executor Executor to use for running parallel fetch / swaps
     * @param adminClient The admin client to use for querying
     * @param timeoutMs Time out in ms
     * @param deleteFailedFetch Boolean to indicate we want to delete data on
     *        successful nodes after a fetch fails somewhere
     * @param rollbackFailedSwap Boolean to indicate we want to rollback the
     *        data on successful nodes after a swap fails somewhere
     */
    public AdminStoreSwapper(Cluster cluster,
                             ExecutorService executor,
                             AdminClient adminClient,
                             long timeoutMs,
                             boolean deleteFailedFetch,
                             boolean rollbackFailedSwap) {
        super(cluster, executor);
        this.adminClient = adminClient;
        this.timeoutMs = timeoutMs;
        this.deleteFailedFetch = deleteFailedFetch;
        this.rollbackFailedSwap = rollbackFailedSwap;
    }

    /**
     * 
     * @param cluster The cluster metadata
     * @param executor Executor to use for running parallel fetch / swaps
     * @param adminClient The admin client to use for querying
     * @param timeoutMs Time out in ms
     */
    public AdminStoreSwapper(Cluster cluster,
                             ExecutorService executor,
                             AdminClient adminClient,
                             long timeoutMs) {
        super(cluster, executor);
        this.adminClient = adminClient;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void invokeRollback(final String storeName, final long pushVersion) {
        Exception exception = null;
        for(Node node: cluster.getNodes()) {
            try {
                logger.info("Invoking rollback for node " + node.getId() + " on store " + storeName
                            + " and version " + pushVersion);
                adminClient.rollbackStore(node.getId(), storeName, pushVersion);
                logger.info("Rollback succeeded for node " + node.getId() + " on store "
                            + storeName + " and version " + pushVersion);
            } catch(Exception e) {
                exception = e;
                logger.error("Exception thrown during rollback operation for node " + node.getId()
                             + " on store " + storeName + " and version " + pushVersion, e);
            }
        }

        if(exception != null)
            throw new VoldemortException(exception);

    }

    @Override
    public List<String> invokeFetch(final String storeName,
                                    final String basePath,
                                    final long pushVersion) {
        // do fetch
        Map<Integer, Future<String>> fetchDirs = new HashMap<Integer, Future<String>>();
        for(final Node node: cluster.getNodes()) {
            fetchDirs.put(node.getId(), executor.submit(new Callable<String>() {

                public String call() throws Exception {
                    String storeDir = basePath + "/node-" + node.getId();
                    logger.info("Invoking fetch for node " + node.getId() + " on store "
                                + storeName + " and store directory " + storeDir);
                    String response = adminClient.fetchStore(node.getId(),
                                                             storeName,
                                                             storeDir,
                                                             pushVersion,
                                                             timeoutMs);
                    if(response == null) {
                        throw new VoldemortException("Fetch request failed for node "
                                                     + node.getId() + " on store " + storeName
                                                     + " and store directory " + storeDir);
                    }
                    logger.info("Fetch succeeded for node " + node.getId() + " on store "
                                + storeName + " and store directory " + storeDir);
                    return response.trim();

                }
            }));
        }

        // wait for all operations to complete successfully
        TreeMap<Integer, String> results = Maps.newTreeMap();
        HashMap<Integer, Exception> exceptions = Maps.newHashMap();

        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            Future<String> val = fetchDirs.get(nodeId);
            try {
                results.put(nodeId, val.get());
            } catch(Exception e) {
                exceptions.put(nodeId, new VoldemortException(e));
            }
        }

        if(!exceptions.isEmpty()) {

            if(deleteFailedFetch) {
                // Delete data from successful nodes
                for(int successfulNodeId: results.keySet()) {
                    try {
                        logger.info("Invoking deletion of fetched data for node "
                                    + successfulNodeId + " on store " + storeName
                                    + " and store directory " + results.get(successfulNodeId));

                        adminClient.failedFetchStore(successfulNodeId,
                                                     storeName,
                                                     results.get(successfulNodeId));
                        logger.info("Deletion succeeded on fetched data for node "
                                    + successfulNodeId + " on store " + storeName
                                    + " and store directory " + results.get(successfulNodeId));

                    } catch(Exception e) {
                        logger.error("Delete request failed for node " + successfulNodeId
                                     + " on store " + storeName + " and store directory "
                                     + results.get(successfulNodeId) + " : ", e);
                    }
                }
            }

            // Finally log the errors for the user
            for(int failedNodeId: exceptions.keySet()) {
                logger.error("Error on node " + failedNodeId + " during push for store "
                             + storeName + " : ", exceptions.get(failedNodeId));
            }

            throw new VoldemortException("Exception during pushes to nodes "
                                         + Joiner.on(",").join(exceptions.keySet()) + " for store "
                                         + storeName + " failed");
        }

        return Lists.newArrayList(results.values());
    }

    @Override
    public void invokeSwap(final String storeName, final List<String> fetchFiles) {
        // do swap
        Map<Integer, String> previousDirs = new HashMap<Integer, String>();
        HashMap<Integer, Exception> exceptions = Maps.newHashMap();

        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            try {
                String dir = fetchFiles.get(nodeId);
                logger.info("Invoking swap for node " + nodeId + " on store " + storeName
                            + " and store directory " + dir);
                previousDirs.put(nodeId, adminClient.swapStore(nodeId, storeName, dir));
                logger.info("Swap succeeded for node " + nodeId + " on store " + storeName
                            + " and store directory " + dir);
            } catch(Exception e) {
                exceptions.put(nodeId, e);
            }
        }

        if(!exceptions.isEmpty()) {

            if(rollbackFailedSwap) {
                // Rollback data on successful nodes
                for(int successfulNodeId: previousDirs.keySet()) {
                    try {
                        logger.info("Invoking rollback ( post failed swap ) for node "
                                    + successfulNodeId + " on store " + storeName
                                    + " and store directory " + previousDirs.get(successfulNodeId));
                        adminClient.rollbackStore(successfulNodeId,
                                                  storeName,
                                                  ReadOnlyUtils.getVersionId(new File(previousDirs.get(successfulNodeId))));
                        logger.info("Rollback ( post failed swap ) succeeded for node "
                                    + successfulNodeId + " on store " + storeName
                                    + " and store directory " + previousDirs.get(successfulNodeId));
                    } catch(Exception e) {
                        logger.error("Exception during rollback ( post failed swap ) operation for node "
                                             + successfulNodeId
                                             + " on store "
                                             + storeName
                                             + " and store directory "
                                             + previousDirs.get(successfulNodeId),
                                     e);
                    }
                }
            }

            // Finally log the errors for the user
            for(int failedNodeId: exceptions.keySet()) {
                logger.error("Error on node " + failedNodeId + " during swap for store "
                             + storeName + " : ", exceptions.get(failedNodeId));
            }

            throw new VoldemortException("Exception during swaps on nodes "
                                         + Joiner.on(",").join(exceptions.keySet()) + " for store "
                                         + storeName + " failed");
        }

    }
}
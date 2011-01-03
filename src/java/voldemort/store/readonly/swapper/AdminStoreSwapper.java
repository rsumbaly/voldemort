package voldemort.store.readonly.swapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.RebalanceUtils;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class AdminStoreSwapper extends StoreSwapper {

    private static final Logger logger = Logger.getLogger(AdminStoreSwapper.class);

    private AdminClient adminClient;
    private long timeoutMs;

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
                logger.info("Attempting rollback for node " + node.getId() + " storeName = "
                            + storeName);
                adminClient.rollbackStore(node.getId(), storeName, pushVersion);
                logger.info("Rollback succeeded for node " + node.getId());
            } catch(Exception e) {
                exception = e;
                logger.error("Exception thrown during rollback operation on node " + node.getId()
                             + ": ", e);
            }
        }

        if(exception != null)
            throw new VoldemortException(exception);

    }

    @Override
    protected List<String> invokeFetch(final String storeName,
                                       final String basePath,
                                       final long pushVersion) {
        // do fetch
        Map<Integer, Future<String>> fetchDirs = new HashMap<Integer, Future<String>>();
        for(final Node node: cluster.getNodes()) {
            fetchDirs.put(node.getId(), executor.submit(new Callable<String>() {

                public String call() throws Exception {
                    String storeDir = basePath + "/node-" + node.getId();
                    logger.info("Invoking fetch for node " + node.getId() + " for " + storeDir);
                    String response = adminClient.fetchStore(node.getId(),
                                                             storeName,
                                                             storeDir,
                                                             pushVersion,
                                                             timeoutMs);
                    if(response == null)
                        throw new VoldemortException("Swap request on node " + node.getId() + " ("
                                                     + node.getHost() + ") failed");
                    logger.info("Fetch succeeded on node " + node.getId());
                    return response.trim();

                }
            }));
        }

        // wait for all operations to complete successfully
        List<String> results = new ArrayList<String>();
        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            Future<String> val = fetchDirs.get(nodeId);
            try {
                results.add(val.get());
            } catch(ExecutionException e) {
                throw new VoldemortException(e.getCause());
            } catch(InterruptedException e) {
                throw new VoldemortException(e);
            }
        }

        return results;
    }

    @Override
    protected void invokeSwap(String storeName, List<String> fetchFiles) {
        // do swap
        Exception exception = null;
        for(int nodeId = 0; nodeId < cluster.getNumberOfNodes(); nodeId++) {
            try {
                String dir = fetchFiles.get(nodeId);
                logger.info("Attempting swap for node " + nodeId + " dir = " + dir);
                adminClient.swapStore(nodeId, storeName, dir);
                logger.info("Swap succeeded for node " + nodeId);
            } catch(Exception e) {
                exception = e;
                logger.error("Exception thrown during swap operation on node " + nodeId + ": ", e);
            }
        }

        if(exception != null)
            throw new VoldemortException(exception);
    }

    public void invokeUpdateClusterMetadata() {
        Versioned<Cluster> latestCluster = new Versioned<Cluster>(adminClient.getAdminClientCluster());
        List<Integer> requiredNodeIds = new ArrayList<Integer>();

        ArrayList<Versioned<Cluster>> clusterList = new ArrayList<Versioned<Cluster>>();
        clusterList.add(latestCluster);

        boolean sameCluster = true;
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            requiredNodeIds.add(node.getId());
            try {
                Versioned<Cluster> versionedCluster = adminClient.getRemoteCluster(node.getId());
                VectorClock newClock = (VectorClock) versionedCluster.getVersion();
                if(null != newClock && !clusterList.contains(versionedCluster)) {

                    if(sameCluster
                       && !adminClient.getAdminClientCluster().equals(versionedCluster.getValue())) {
                        sameCluster = false;
                    }
                    // check no two clocks are concurrent.
                    RebalanceUtils.checkNotConcurrent(clusterList, newClock);

                    // add to clock list
                    clusterList.add(versionedCluster);

                    // update latestClock
                    Occured occured = newClock.compare(latestCluster.getVersion());
                    if(Occured.AFTER.equals(occured))
                        latestCluster = versionedCluster;
                }
            } catch(Exception e) {
                throw new VoldemortException("Failed to get cluster metadata from node:" + node, e);
            }
        }

        if(!sameCluster) {
            VectorClock latestClock = (VectorClock) latestCluster.getVersion();
            latestClock.incrementVersion(cluster.getNodes().iterator().next().getId(),
                                         System.currentTimeMillis());
            RebalanceUtils.propagateCluster(adminClient, cluster, latestClock, requiredNodeIds);
        }
    }
}
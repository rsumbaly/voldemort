/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed.action;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.secondary.RangeQuery;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.GetAllKeysPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.utils.ByteArray;

import com.google.common.collect.MapMaker;

public class PerformParallelGetAllKeysRequests extends
        AbstractAction<ByteArray, List<ByteArray>, GetAllKeysPipelineData> {

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    private final RangeQuery query;

    public PerformParallelGetAllKeysRequests(GetAllKeysPipelineData pipelineData,
                                             FailureDetector failureDetector,
                                             long timeoutMs,
                                             Map<Integer, NonblockingStore> nonblockingStores,
                                             RangeQuery query) {
        super(pipelineData, Event.COMPLETED);
        this.failureDetector = failureDetector;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
        this.query = query;
    }

    public void execute(final Pipeline pipeline) {
        int attempts = pipelineData.getNodes().size();
        final CountDownLatch latch = new CountDownLatch(attempts);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        final Map<Node, Set<ByteArray>> responses = new MapMaker().makeMap();

        for(final Node node: pipelineData.getNodes()) {
            NonblockingStoreCallback callback = new NonblockingStoreCallback() {

                public void requestComplete(Object result, long requestTime) {
                    if(logger.isTraceEnabled())
                        logger.trace(pipeline.getOperation().getSimpleName()
                                     + " response received (" + requestTime + " ms.) from node "
                                     + node.getId());

                    responses.put(node, (Set<ByteArray>) result);
                    latch.countDown();
                }

            };

            if(logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                             + " request on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());
            store.submitGetAllKeysRequest(query, null, callback, timeoutMs); // TODO
                                                                             // transforms?
        }

        try {
            latch.await(timeoutMs * 3, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        // Merge results
        // TODO this should be done on-the-fly to avoid storing all reponses
        for(Map.Entry<Node, Set<ByteArray>> entry: responses.entrySet()) {
            pipelineData.getResult().addAll(entry.getValue());
        }

        // for(Response<Iterable<ByteArray>, Object> response:
        // responses.values()) {
        // if(response.getValue() instanceof Exception) {
        // if(handleResponseError(response, pipeline, failureDetector))
        // return;
        // } else {
        // Map<ByteArray, List<Versioned<byte[]>>> values = (Map<ByteArray,
        // List<Versioned<byte[]>>>) response.getValue();
        //
        // for(ByteArray key: response.getKey()) {
        // MutableInt successCount = pipelineData.getSuccessCount(key);
        // successCount.increment();
        //
        // List<Versioned<byte[]>> retrieved = values.get(key);
        // /*
        // * retrieved can be null if there are no values for the key
        // * provided
        // */
        // if(retrieved != null) {
        // List<Versioned<byte[]>> existing = pipelineData.getResult().get(key);
        //
        // if(existing == null)
        // pipelineData.getResult().put(key, Lists.newArrayList(retrieved));
        // else
        // existing.addAll(retrieved);
        // }
        //
        // HashSet<Integer> zoneResponses = null;
        // if(pipelineData.getKeyToZoneResponse().containsKey(key)) {
        // zoneResponses = pipelineData.getKeyToZoneResponse().get(key);
        // } else {
        // zoneResponses = new HashSet<Integer>();
        // }
        // zoneResponses.add(response.getNode().getZoneId());
        // }
        //
        // pipelineData.getResponses()
        // .add(new Response<Iterable<ByteArray>, Map<ByteArray,
        // List<Versioned<byte[]>>>>(response.getNode(),
        // response.getKey(),
        // values,
        // response.getRequestTime()));
        // failureDetector.recordSuccess(response.getNode(),
        // response.getRequestTime());
        // }
        // }

        pipeline.addEvent(completeEvent);
    }
}
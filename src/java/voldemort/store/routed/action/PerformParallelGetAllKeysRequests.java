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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;

import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.secondary.RangeQuery;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.nonblockingstore.NonblockingStoreCallback;
import voldemort.store.routed.GetAllKeysPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.Response;
import voldemort.utils.ByteArray;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;

public class PerformParallelGetAllKeysRequests extends
        AbstractAction<ByteArray, Set<ByteArray>, GetAllKeysPipelineData> {

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    private final RangeQuery query;

    private final int requiredTotal;

    private final int requiredPerKey;

    public PerformParallelGetAllKeysRequests(GetAllKeysPipelineData pipelineData,
                                             FailureDetector failureDetector,
                                             long timeoutMs,
                                             Map<Integer, NonblockingStore> nonblockingStores,
                                             RangeQuery query,
                                             int requiredTotal,
                                             int requiredPerKey) {
        super(pipelineData, Event.COMPLETED);
        this.failureDetector = failureDetector;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
        this.query = query;
        this.requiredTotal = requiredTotal;
        this.requiredPerKey = requiredPerKey;
    }

    public void execute(final Pipeline pipeline) {
        int attempts = pipelineData.getNodes().size();
        final CountDownLatch latch = new CountDownLatch(attempts);

        if(logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        final Map<Node, Response<RangeQuery, Object>> responsesByNode = new MapMaker().makeMap();

        for(final Node node: pipelineData.getNodes()) {
            NonblockingStoreCallback callback = new NonblockingStoreCallback() {

                public void requestComplete(Object result, long requestTime) {
                    if(logger.isTraceEnabled())
                        logger.trace(pipeline.getOperation().getSimpleName()
                                     + " response received (" + requestTime + " ms.) from node "
                                     + node.getId());

                    Response<RangeQuery, Object> response = new Response<RangeQuery, Object>(node,
                                                                                             query,
                                                                                             result,
                                                                                             requestTime);
                    responsesByNode.put(node, response);
                    latch.countDown();

                    // Note errors that come in after the pipeline has finished.
                    // These will *not* get a chance to be called in the loop of
                    // responses below.
                    if(pipeline.isFinished() && response.getValue() instanceof Exception)
                        handleResponseError(response, pipeline, failureDetector);
                }

            };

            if(logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                             + " request on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());
            store.submitGetAllKeysRequest(query, null, callback, timeoutMs);
        }

        try {
            latch.await(timeoutMs * 3, TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            if(logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        // Merge results
        // TODO this should be done on-the-fly to avoid storing all responses in
        // the caller machine
        Multiset<ByteArray> responses = HashMultiset.create();
        for(Response<RangeQuery, Object> response: responsesByNode.values()) {
            if(response.getValue() instanceof Exception) {
                if(handleResponseError(response, pipeline, failureDetector))
                    return;
            } else {
                @SuppressWarnings("unchecked")
                Set<ByteArray> keys = (Set<ByteArray>) response.getValue();
                responses.addAll(keys);
                pipelineData.incrementSuccesses();
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
            }
        }

        if(pipelineData.getSuccesses() < requiredTotal) {
            pipelineData.setFatalError(new InsufficientOperationalNodesException(requiredTotal
                                                                                         + " "
                                                                                         + pipeline.getOperation()
                                                                                                   .getSimpleName()
                                                                                         + "s required, but "
                                                                                         + pipelineData.getSuccesses()
                                                                                         + " succeeded",
                                                                                 pipelineData.getFailures()));
            pipeline.addEvent(Event.ERROR);
            return;
        }

        // filter by number of responses, to discard partial deletes
        for(Entry<ByteArray> entry: responses.entrySet()) {
            if(entry.getCount() >= requiredPerKey)
                pipelineData.getResult().add(entry.getElement());
        }

        pipeline.addEvent(completeEvent);
    }
}
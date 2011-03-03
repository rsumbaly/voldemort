package voldemort.store.routed;

import java.util.List;
import java.util.Set;

import voldemort.cluster.Node;
import voldemort.utils.ByteArray;

import com.google.common.collect.Sets;

public class GetAllKeysPipelineData extends PipelineData<ByteArray, List<ByteArray>> {

    private List<Node> nodes;

    private final Set<ByteArray> result = Sets.newHashSet();

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public Set<ByteArray> getResult() {
        return result;
    }

}

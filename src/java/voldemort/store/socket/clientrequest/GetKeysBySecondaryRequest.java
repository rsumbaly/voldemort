package voldemort.store.socket.clientrequest;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

import voldemort.client.protocol.RequestFormat;
import voldemort.secondary.RangeQuery;
import voldemort.server.RequestRoutingType;
import voldemort.utils.ByteArray;

public class GetKeysBySecondaryRequest extends AbstractStoreClientRequest<Set<ByteArray>> {

    private final RangeQuery query;

    public GetKeysBySecondaryRequest(String storeName,
                                     RequestFormat requestFormat,
                                     RequestRoutingType requestRoutingType,
                                     RangeQuery query) {
        super(storeName, requestFormat, requestRoutingType);
        this.query = query;
    }

    public boolean isCompleteResponse(ByteBuffer buffer) {
        return requestFormat.isCompleteGetKeysBySecondaryResponse(buffer);
    }

    @Override
    protected void formatRequestInternal(DataOutputStream outputStream) throws IOException {
        requestFormat.writeGetKeysBySecondary(outputStream, storeName, query, requestRoutingType);
    }

    @Override
    protected Set<ByteArray> parseResponseInternal(DataInputStream inputStream) throws IOException {
        return requestFormat.readGetKeysBySecondaryResponse(inputStream);
    }

}

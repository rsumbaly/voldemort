/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.serialized;

import voldemort.serialization.StringSerializer;
import voldemort.store.AbstractStringStoreTest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;

/**
 * 
 */
public class SerializingStoreTest extends AbstractStringStoreTest {

    @Override
    public Store<String, String, String> getStore() {
        return SerializingStore.wrap(new InMemoryStorageEngine<ByteArray, byte[], byte[]>("test"),
                                     new StringSerializer(),
                                     new StringSerializer(),
                                     new StringSerializer());
    }

}

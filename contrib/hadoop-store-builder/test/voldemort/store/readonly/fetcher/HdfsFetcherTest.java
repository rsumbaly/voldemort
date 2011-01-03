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

package voldemort.store.readonly.fetcher;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import junit.framework.TestCase;
import voldemort.TestUtils;
import voldemort.store.readonly.checksum.CheckSumTests;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;

/**
 * Tests for the HDFS-based fetcher
 * 
 * 
 */
public class HdfsFetcherTest extends TestCase {

    public void testFetch() throws Exception {
        File testSourceDirectory = TestUtils.createTempDir();
        File testDestinationDirectory = TestUtils.createTempDir();

        File testFile = File.createTempFile("test", ".dat", testSourceDirectory);
        testFile.createNewFile();

        // Test 1: No checksum file - return correctly
        // Required for backward compatibility with existing hadoop stores
        HdfsFetcher fetcher = new HdfsFetcher();
        File fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                         testDestinationDirectory.getAbsolutePath() + "1");
        assertNotNull(fetchedFile);

        // Test 2: Add checksum file with incorrect fileName, should not fail
        File checkSumFile = new File(testSourceDirectory, "blahcheckSum.txt");
        checkSumFile.createNewFile();
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "2");
        assertNotNull(fetchedFile);
        checkSumFile.delete();

        // Test 3: Add checksum file with correct fileName, but empty = wrong
        // md5
        checkSumFile = new File(testSourceDirectory, "adler32checkSum.txt");
        checkSumFile.createNewFile();
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "3");
        assertNull(fetchedFile);

        // Test 4: Add wrong contents to file i.e. contents of CRC32 instead of
        // Adler
        byte[] checkSumBytes = CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                                                               CheckSumType.CRC32);
        DataOutputStream os = new DataOutputStream(new FileOutputStream(checkSumFile));
        os.write(checkSumBytes);
        os.close();
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "4");
        assertNull(fetchedFile);
        checkSumFile.delete();

        // Test 5: Add correct checksum contents - MD5
        checkSumFile = new File(testSourceDirectory, "md5checkSum.txt");
        byte[] checkSumBytes2 = CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                                                                CheckSumType.MD5);
        os = new DataOutputStream(new FileOutputStream(checkSumFile));
        os.write(checkSumBytes2);
        os.close();
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "5");
        assertNotNull(fetchedFile);
        checkSumFile.delete();

        // Test 6: Add correct checksum contents - ADLER32
        checkSumFile = new File(testSourceDirectory, "adler32checkSum.txt");
        byte[] checkSumBytes3 = CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                                                                CheckSumType.ADLER32);
        os = new DataOutputStream(new FileOutputStream(checkSumFile));
        os.write(checkSumBytes3);
        os.close();
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "6");
        assertNotNull(fetchedFile);
        checkSumFile.delete();

        // Test 7: Add correct checksum contents - CRC32
        checkSumFile = new File(testSourceDirectory, "crc32checkSum.txt");
        byte[] checkSumBytes4 = CheckSumTests.calculateCheckSum(testSourceDirectory.listFiles(),
                                                                CheckSumType.CRC32);
        os = new DataOutputStream(new FileOutputStream(checkSumFile));
        os.write(checkSumBytes4);
        os.close();
        fetchedFile = fetcher.fetch(testSourceDirectory.getAbsolutePath(),
                                    testDestinationDirectory.getAbsolutePath() + "7");
        assertNotNull(fetchedFile);
        checkSumFile.delete();

    }
}

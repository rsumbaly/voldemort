package voldemort.utils;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;
import voldemort.VoldemortException;

public class JNAUtilsTest extends TestCase {

    public void testSymlink() throws IOException {
        String tempParentDir = System.getProperty("java.io.tmpdir");
        File tempDir = new File(tempParentDir, "temp" + (System.currentTimeMillis() + 1));
        File tempSymLink = new File(tempParentDir, "link" + (System.currentTimeMillis() + 2));
        File tempDir2 = new File(tempParentDir, "temp" + (System.currentTimeMillis() + 3));

        // Test against non-existing directory
        assertTrue(!tempDir.exists());
        try {
            JNAUtils.createSymlink(tempDir.getAbsolutePath(), tempSymLink.getAbsolutePath());
            fail("Symlink should have thrown an exception since directory did not exist");
        } catch(VoldemortException e) {}

        // Normal test
        Utils.mkdirs(tempDir);
        try {
            JNAUtils.createSymlink(tempDir.getAbsolutePath(), tempSymLink.getAbsolutePath());
        } catch(VoldemortException e) {
            fail("Test against non-existing symlink");
        }

        assertTrue(!Utils.isSymLink(tempDir));
        assertTrue(Utils.isSymLink(tempSymLink));

        // Test if existing sym-link can switch to new directory
        Utils.mkdirs(tempDir2);
        try {
            JNAUtils.createSymlink(tempDir2.getAbsolutePath(), tempSymLink.getAbsolutePath());
        } catch(VoldemortException e) {
            e.printStackTrace();
            fail("Test against already existing symlink ");
        }
        assertTrue(Utils.isSymLink(tempSymLink));
        // Check if it was not deleted with sym-link
        assertTrue(tempDir.exists());

        File dumbFile = new File(tempDir2, "dumbFile");
        dumbFile.createNewFile();
        assertEquals(1, tempSymLink.list().length);
    }
}

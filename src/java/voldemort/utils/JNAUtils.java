package voldemort.utils;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;

/**
 * Util functions wrapped around JNA calls
 * 
 * For more information about posix_fadvise -
 * http://www.devshed.com/c/a/BrainDump/Advising-the-Linux-Kernel-on-File-IO/
 * 
 * For fcntl function
 * http://www.opensource.apple.com/source/xnu/xnu-201.42.3/bsd/sys/fcntl.h
 * 
 */
@SuppressWarnings("unused")
public class JNAUtils {

    private static Logger logger = Logger.getLogger(JNAUtils.class);

    public interface Posix extends Library {

        /**
         * Native access to fcntl ( file control )
         * 
         * @param fd File descriptor
         * @param command Advisory command locking modes
         * @param flags File descriptor flags
         * @return Return code
         * @throws LastErrorException
         */
        public int fcntl(int fd, int command, long flags);

        /**
         * Native access to posix_fadvise
         * 
         * @param fd File descriptor
         * @param offset Offset of file
         * @param len Length of file
         * @param advise Advice type
         * @return Return code
         * @throws LastErrorException
         */
        public int posix_fadvise(int fd, int offset, int len, int advise);

        /**
         * Native access to symlink function
         * 
         * @param filePath File path
         * @param symLinkPath Location of sym link we want to create
         * @return Return code
         * @throws LastErrorException
         */
        public int symlink(String filePath, String symLinkPath);
    }

    private static Posix posix = (Posix) Native.loadLibrary("c", Posix.class);

    /**
     * Preallocate file storage space. Note: upon success, the space that is
     * allocated can be the same size or larger than the space requested.
     */
    private static final int F_PREALLOCATE = 42;

    /**
     * Issue an advisory read async with no copy to user.
     */
    private static final int F_RDADVISE = 44;

    /**
     * Turn read ahead off/on. A zero value in arg disables read ahead. A
     * non-zero value in arg turns read ahead on.
     */
    private static final int F_RDAHEAD = 45;

    /**
     * Turning data caching off/on
     */
    private static final int F_NOCACHE = 48;

    /**
     * The application intends to access the data in the specified range in a
     * random (nonsequential) order. The kernel disables readahead, reading only
     * the minimal amount of data on each physical read operation.
     */
    private static final int POSIX_FADV_RANDOM = 1;

    /**
     * The application intends to access the data in the specified range
     * sequentially, from lower to higher addresses. The kernel performs
     * aggressive readahead, doubling the size of the readahead window.
     */
    private static final int POSIX_FADV_SEQUENTIAL = 2;

    /**
     * The application intends to access the data in the specified range in the
     * near future. The kernel initiates readahead to begin reading into memory
     * the given pages.
     */
    private static final int POSIX_FADV_WILLNEED = 3;

    /**
     * Create a symbolic link to an existing file. Also deletes the existing
     * symbolic link if it exists
     * 
     * @param filePath Path of the file for whom to create the symbolic link
     * @param symLinkPath Path of the symbolic link
     */
    public static void createSymlink(String filePath, String symLinkPath) {
        File file = new File(filePath);
        File symLink = new File(symLinkPath);
        symLink.delete();

        if(!file.exists())
            throw new VoldemortException("File " + filePath + " does not exist");

        int returnCode = posix.symlink(filePath, symLinkPath);
        if(returnCode < 0)
            throw new VoldemortException("Unable to create symbolic link for " + filePath);
    }

    /**
     * Given the input stream of the file and its length, tries to hint the OS
     * to keep the content in page cache.
     * 
     * @param stream Stream of file we want in page cache
     * @param length The length of the file
     */
    public static void enableCaching(FileInputStream stream, long length) {
        Field field = null;
        int fd = -1;
        try {
            field = stream.getFD().getClass().getDeclaredField("fd");
            field.setAccessible(true);
            fd = field.getInt(stream.getFD());
        } catch(Exception e) {
            logger.warn("Could not retrieve file descriptor successfully. Cannot run optimization",
                        e);
            return;
        }

        if(fd == -1) {
            logger.warn("File descriptor returned is invalid. Cannot run optimization");
            return;
        }

        try {
            if(System.getProperty("os.name").toLowerCase().contains("linux")) {
                logger.info("Running fadvise");
                posix.posix_fadvise(fd, 0, (int) length, POSIX_FADV_WILLNEED);
            } else if(System.getProperty("os.name").toLowerCase().contains("mac")) {
                logger.info("Running fcntl");
                posix.fcntl(fd, F_PREALLOCATE, 1);
            }
        } catch(Exception e) {
            logger.warn("Could not successfully enable optimized caching", e);
        }
    }
}

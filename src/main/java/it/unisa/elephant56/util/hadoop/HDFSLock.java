package it.unisa.elephant56.util.hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Mutual exclusion distributed lock based on creation and deletion files using HDFS. It provides basic lock behaviours.
 * When the method to acquire the lock is called a temporary file is created.The temporary file is used to acquire the
 * lock by renaming the file with the common name file of the lock.
 * When the lock is acquired with the current temporary file, this does not exist anymore.
 * When the unlock method is called the lock file name is deleted.
 */
public class HDFSLock {
    public static final long DEFAULT_TIME_TO_WAIT = 1000L;
    public static final String TEMP_FILE_EXTENSION = "tmp";

    private FileSystem fileSystem;
    private Path lockFilePath;
    private Path tempLockFilePath;

    /**
     * Creates an instance of lock with the given fairness policy.
     *
     * @param lockFilePath the file path where the lock will be acquired
     * @param fileSystem filesystem where the will be created
     * @param id for the temporary lock name file
     */
    public HDFSLock(Path lockFilePath, FileSystem fileSystem, String id) {
        this.lockFilePath = lockFilePath;
        this.fileSystem = fileSystem;
        this.tempLockFilePath = new Path(this.lockFilePath.toString() + "." + id + "." + TEMP_FILE_EXTENSION);
    }

    /**
     * Acquires the lock only if it is free at the time of invocation.
     *
     * @return true if the lock has been acquired, false otherwise
     *
     * @throws IOException
     */
    public boolean tryLock() throws IOException {
        this.createTempLock();
        return fileSystem.rename(this.tempLockFilePath, this.lockFilePath);
    }

    /**
     * Acquires the lock if it is free.
     * If the lock is not available waits custom time before trying again.
     *
     * @param timeToWait time to wait before trying again
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void lock(long timeToWait) throws IOException, InterruptedException {
        this.createTempLock();
        while (!this.fileSystem.rename(this.tempLockFilePath, this.lockFilePath))
            Thread.sleep(timeToWait);
    }

    /**
     * Acquires the lock if it is free.
     * If the lock is not available waits @DEFAULT_TIME_TO_WAIT before trying again.
     *
     * @return "true" if the lock has been acquired, "false" otherwise
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void lock() throws IOException, InterruptedException {
        this.createTempLock();
        while (!fileSystem.rename(this.tempLockFilePath, this.lockFilePath))
            Thread.sleep(DEFAULT_TIME_TO_WAIT);
    }

    /**
     * Releases the lock.
     *
     * @throws IOException
     */
    public void unlock() throws IOException {
        this.fileSystem.delete(this.lockFilePath, false);
    }

    /**
     * Create the temporary lock before renaming.
     *
     * @throws IOException
     */
    private void createTempLock() throws IOException {
        FSDataOutputStream tempLockFileOutputStream = fileSystem.create(this.tempLockFilePath);
        tempLockFileOutputStream.close();
    }

    /**
     * Get the path of temporary lock file.
     *
     * @return the path of the temporary lock file
     */
    public Path getTempLockFileName() {
        return this.tempLockFilePath;
    }

    /**
     * Get the path of lock file.
     *
     * @return the path of the lock file
     */
    public Path getPath() {
        return this.lockFilePath;
    }
}

package it.unisa.elephant56.core.generator;

import it.unisa.elephant56.core.Constants;
import it.unisa.elephant56.core.common.Properties;
import it.unisa.elephant56.util.hadoop.FilesPathFilter;
import it.unisa.elephant56.util.hadoop.HDFSLock;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IslandDistributedGenerationsBlockExecutor extends GenerationsBlockExecutor {
    public static final FilesPathFilter terminationFlagFilesPathFilter =
            new FilesPathFilter(null, Constants.TERMINATION_FLAG_FILE_EXTENSION);

    public static boolean checkTerminationConditionSatisfaction(
            Path terminationFlagFilesFolderPath, FileSystem fileSystem) {
        // Check if a the least a file exists.
        FileStatus[] files;
        try {
            files = fileSystem.listStatus(terminationFlagFilesFolderPath, terminationFlagFilesPathFilter);
        } catch (IOException exception) {
            exception.printStackTrace();
            return false;
        }
        return (files.length > 0);
    }

    private Path terminationFlagFilesFolderPath;

    private Path islandsPropertiesFilesFolderPath;
    private PathFilter propertiesFilesFilter;

    private FileSystem fileSystem;

    private String generationNameFormat;

    public IslandDistributedGenerationsBlockExecutor(
            Path terminationFlagFilesFolderPath,
            Path islandsPropertiesFilesFolderPath,
            FileSystem fileSystem,
            String generationNameFormat
    ) {
        super();

        this.terminationFlagFilesFolderPath = terminationFlagFilesFolderPath;

        this.islandsPropertiesFilesFolderPath = islandsPropertiesFilesFolderPath;
        this.propertiesFilesFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return (path.getName().endsWith("." + Constants.PROPERTIES_FILES_EXTENSION));
            }
        };

        this.fileSystem = fileSystem;

        this.generationNameFormat = generationNameFormat;
    }

    @Override
    public boolean checkTerminationConditionSatisfactionNotifications() {
        // TODO Riabilitare accesso HDFS.
        //return checkTerminationConditionSatisfaction(this.terminationFlagFilesFolderPath, this.fileSystem);
        return false;
    }

    @Override
    public void notifyTerminationConditionSatisfaction() {
        // Retrieves the path for the flag file of the current island.
        Path terminationFlagPath = new Path(this.terminationFlagFilesFolderPath, this.getNodeNumber() + "." +
                Constants.TERMINATION_FLAG_FILE_EXTENSION);

        // Writes the empty file.
        FSDataOutputStream flagFile;
        try {
            flagFile = fileSystem.create(terminationFlagPath, true);
            flagFile.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public boolean shareIslandProperties(Properties properties, long generationNumber) {
        boolean isLast = false;

        try {
            // Creates the generation folder.
            Path generationPropertiesFolderPath = new Path(this.islandsPropertiesFilesFolderPath,
                    String.format(this.generationNameFormat, generationNumber));
            this.fileSystem.mkdirs(generationPropertiesFolderPath);

            // Acquires the lock.
            Path lockFilePath = new Path(generationPropertiesFolderPath, Constants.LOCK_FILE_NAME);
            HDFSLock lock = new HDFSLock(lockFilePath, this.fileSystem, Integer.toString(this.getNodeNumber()));
            try {
                lock.lock(Constants.LOCK_TIME_TO_WAIT);
            } catch (InterruptedException exception) {
                exception.printStackTrace();
                System.exit(-1);
            }

            // Checks if it is the last island.
            FileStatus[] files = fileSystem.listStatus(generationPropertiesFolderPath, this.propertiesFilesFilter);
            if (files.length == (this.getTotalNumberOfNodes() - 1))
                isLast = true;

            // Writes the properties if it is not the last island.
            if (!isLast) {
                Path propertiesFilePath = new Path(generationPropertiesFolderPath,
                        String.format(Constants.ISLANDS_NAME_FORMAT, this.getNodeNumber()) +
                                "." + Constants.PROPERTIES_FILES_EXTENSION);
                properties.writeToFile(propertiesFilePath, this.fileSystem);
            }

            // Releases the lock.
            lock.unlock();
        } catch (IOException exception) {
            exception.printStackTrace();
        }

        return isLast;
    }

    @Override
    public List<Properties> readSharedIslandProperties(Properties islandProperties, long generationNumber) {
        // Constructs the list of properties.
        List<Properties> result = new ArrayList<Properties>(this.getTotalNumberOfNodes());

        try {
            Path generationPropertiesFolderPath = new Path(this.islandsPropertiesFilesFolderPath,
                    String.format(this.generationNameFormat, generationNumber));

            FileStatus[] files = fileSystem.listStatus(generationPropertiesFolderPath, this.propertiesFilesFilter);
            for (int i = 0; i < this.getTotalNumberOfNodes(); i++) {
                if (i == this.getNodeNumber()) {
                    result.add(islandProperties);
                } else {
                    result.add(Properties.readFromFile(files[i].getPath(), this.fileSystem));
                }
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
        return result;
    }
}

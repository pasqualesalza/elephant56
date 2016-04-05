package it.unisa.elephant56.core.reporter.time;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import it.unisa.elephant56.user.sample.common.fitness_value.IntegerFitnessValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

public class MapReduceTimeReporter extends TimeReporter {
    /**
     * The header.
     */
    public static final String[] HEADER = {
            "island_number", "generations_block_number", "generation_number", "phase_type",
            "start_time", "finish_time", "total_time", "human_total_time"
    };

    /**
     * The types of phases.
     */
    public static enum PhaseType {
        MAPPER_INITIALISATION,
        MAPPER_COMPUTATION,
        MAPPER_FINALISATION,
        REDUCER_INITIALISATION,
        REDUCER_COMPUTATION,
        REDUCER_FINALISATION
    };

    public static final int ISLAND_NUMBER_PARTIAL_TIMES_HEADER_INDEX = 0;
    public static final int GENERATIONS_BLOCK_NUMBER_PARTIAL_TIMES_HEADER_INDEX = 1;
    public static final int PHASE_TYPE_PARTIAL_TIMES_HEADER_INDEX = 2;
    public static final int TYPE_PARTIAL_TIMES_HEADER_INDEX = 3;
    public static final int TIME_PARTIAL_TIMES_HEADER_INDEX = 4;

    public static class PartialTimeKey {
        /**
         * The types of partial time.
         */
        public static enum Type {
            START,
            FINISH
        }

        private int islandNumber;
        private long generationsBlockNumber;
        private PhaseType phaseType;
        private Type type;

        public PartialTimeKey(int islandNumber, long generationsBlockNumber, PhaseType phaseType, Type type) {
            this.islandNumber = islandNumber;
            this.generationsBlockNumber = generationsBlockNumber;
            this.phaseType = phaseType;
            this.type = type;
        }

        public PartialTimeKey getOpposite() {
            Type oppositeType = (this.type == Type.START) ? Type.FINISH : Type.START;
            PartialTimeKey opposite =
                    new PartialTimeKey(this.islandNumber, this.generationsBlockNumber, this.phaseType, oppositeType);
            return opposite;
        }

        @Override
        public int hashCode() {
            int result = islandNumber;
            result = 31 * result + (int) (generationsBlockNumber ^ (generationsBlockNumber >>> 32));
            result = 31 * result + (phaseType != null ? phaseType.hashCode() : 0);
            result = 31 * result + (type != null ? type.hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartialTimeKey that = (PartialTimeKey) o;

            if (generationsBlockNumber != that.generationsBlockNumber) return false;
            if (islandNumber != that.islandNumber) return false;
            if (phaseType != that.phaseType) return false;
            if (type != that.type) return false;

            return true;
        }
    }

    private Map<PartialTimeKey, Long> partialTimesMap;

    public MapReduceTimeReporter(Path outputFilePath, FileSystem fileSystem) throws IOException {
        super(outputFilePath, fileSystem, HEADER);

        this.partialTimesMap = new HashMap<PartialTimeKey, Long>();
    }

    public void writeTime(
            int islandNumber, long generationsBlockNumber, PhaseType phaseType, long startTime, long finishTime) {
        super.writeTime(islandNumber, generationsBlockNumber, -1L, phaseType.name(), startTime, finishTime);
    }

    public void addPartialTime(int islandNumber, long generationsBlockNumber, PhaseType phaseType,
                               PartialTimeKey.Type partialTimeType, long time) {
        PartialTimeKey partialTimeKey =
                new PartialTimeKey(islandNumber, generationsBlockNumber, phaseType, partialTimeType);

        this.partialTimesMap.put(partialTimeKey, time);
    }

    public void addPartialTimesFromFile(Path inputFilePath, FileSystem fileSystem) throws IOException {
        // Creates the reader.
        FSDataInputStream fileInputStream = fileSystem.open(inputFilePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        CSVReader fileReader = new CSVReader(bufferedReader);

        // Iterates among the lines.
        List<String[]> lines = fileReader.readAll();
        for (String[] line : lines) {
            // Reads data.
            int islandNumber = Integer.parseInt(line[ISLAND_NUMBER_PARTIAL_TIMES_HEADER_INDEX]);
            long generationsBlockNumber = Long.parseLong(line[GENERATIONS_BLOCK_NUMBER_PARTIAL_TIMES_HEADER_INDEX]);
            PhaseType phaseType = PhaseType.valueOf(line[PHASE_TYPE_PARTIAL_TIMES_HEADER_INDEX]);
            PartialTimeKey.Type partialTimeType = PartialTimeKey.Type.valueOf(line[TYPE_PARTIAL_TIMES_HEADER_INDEX]);
            long time = Long.parseLong(line[TIME_PARTIAL_TIMES_HEADER_INDEX]);

            // Adds the time.
            this.addPartialTime(islandNumber, generationsBlockNumber, phaseType, partialTimeType, time);
        }
    }

    public void writePartialTimesToFile(Path outputFilePath, FileSystem fileSystem) throws IOException {
        // Creates the file and the writer.
        FSDataOutputStream fileOutputStream = fileSystem.create(outputFilePath, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
        CSVWriter fileWriter = new CSVWriter(bufferedWriter);

        // Writes partial times.
        for (Map.Entry<PartialTimeKey, Long> mapEntry : partialTimesMap.entrySet()) {
            PartialTimeKey partialTimeKey = mapEntry.getKey();
            long time = mapEntry.getValue();
            this.writeNextPartialTimeToFileWriter(partialTimeKey, time, fileWriter);
        }

        // Finalises the file.
        fileWriter.close();
    }

    private void writeNextPartialTimeToFileWriter(PartialTimeKey partialTimeKey, long time, CSVWriter fileWriter) {
        String[] line = {
                Integer.toString(partialTimeKey.islandNumber),
                Long.toString(partialTimeKey.generationsBlockNumber),
                partialTimeKey.phaseType.name(),
                partialTimeKey.type.name(),
                Long.toString(time)
        };

        fileWriter.writeNext(line);
    }

    public void joinPartialTimes(int islandNumber, long generationsBlockNumber) {
        for (PhaseType phaseType : PhaseType.values()) {
            PartialTimeKey startPartialTime =
                    new PartialTimeKey(islandNumber, generationsBlockNumber, phaseType, PartialTimeKey.Type.START);
            PartialTimeKey finishPartialTime =
                    new PartialTimeKey(islandNumber, generationsBlockNumber, phaseType, PartialTimeKey.Type.FINISH);

            if (this.partialTimesMap.containsKey(startPartialTime) &&
                    this.partialTimesMap.containsKey(startPartialTime)) {
                long startTime = this.partialTimesMap.get(startPartialTime);
                long finishTime = this.partialTimesMap.get(finishPartialTime);
                writeTime(islandNumber, generationsBlockNumber, phaseType, startTime, finishTime);
            }
        }
    }
}

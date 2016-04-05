package it.unisa.elephant56.core.reporter;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

public class Reporter {
    private Path outputFilePath;
    private FileSystem fileSystem;
    private CSVWriter fileWriter;

    private String[] header;

    public Reporter(Path outputFilePath, FileSystem fileSystem, String[] header) throws IOException {
        this.outputFilePath = outputFilePath;
        this.fileSystem = fileSystem;
        this.header = header;
    }

    /**
     * Initialises the file.
     *
     * @throws IOException
     */
    public void initialiseFile() throws IOException {
        // Creates the file and the writer.
        FSDataOutputStream fileOutputStream = this.fileSystem.create(this.outputFilePath, true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
        this.fileWriter = new CSVWriter(bufferedWriter);

        // Writes the header.
        this.fileWriter.writeNext(this.header);
    }

    /**
     * Finalises the open file.
     *
     * @throws IOException
     */
    public void finaliseFile() throws IOException {
        // Closes the writer.
        this.fileWriter.close();
    }

    /**
     * Writes the line.
     *
     * @param line the line
     */
    public void writeLine(String[] line) {
        this.fileWriter.writeNext(line);
    }
}

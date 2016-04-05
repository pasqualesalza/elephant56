package it.unisa.elephant56.core.common;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Defines the properties object that user can use for custom actions.
 */
public class Properties {
    /**
     * Reads the properties from a file.
     *
     * @param inputFilePath the path where to read the properties
     * @param fileSystem the filesystem
     * @return the read properties
     *
     * @throws IOException
     */
    public static Properties readFromFile(Path inputFilePath, FileSystem fileSystem) throws IOException {
        FSDataInputStream fileInputStream = fileSystem.open(inputFilePath);

        Configuration configuration = new Configuration(false);
        configuration.readFields(fileInputStream);

        fileInputStream.close();

        Properties properties = new Properties(configuration);

        return properties;
    }

    private Configuration configuration;

	/**
	 * Constructs the object.
	 */
	public Properties() {
		this.configuration = new Configuration(false);
	}
	
	/**
	 * Constructs the object specifying the configuration.
     *
	 * @param configuration the configuration
	 */
	public Properties(Configuration configuration) {
		this.configuration = configuration;
	}

    /**
     * Copies the properties into a Configuration object.
     *
     * @param configuration the destination
     */
    public void copyIntoConfiguration(Configuration configuration) {
        for (Map.Entry<String, String> entry : this.configuration)
            configuration.set(entry.getKey(), entry.getValue());
    }

    /**
     * Writes the properties into a file.
     *
     * @param outputFilePath the path where to write the properties
     * @param fileSystem the filesystem
     *
     * @throws java.io.IOException
     */
    public void writeToFile(Path outputFilePath, FileSystem fileSystem) throws IOException {
        FSDataOutputStream fileOutputStream = fileSystem.create(outputFilePath, true);
        this.configuration.write(fileOutputStream);
        fileOutputStream.close();
    }

	/**
	 * Checks if a property is set or not.
	 *
	 * @param name the name of the property
	 * @return "true" if yes, "false" otherwise
	 */
	public boolean isSet(String name) {
		return (this.configuration.get(name) != null);
	}

	/**
	 * Get the value of the the "name" property as a String.
	 *
	 * @param name the name of the property
	 * @param defaultValue the default value, if the property doesn't exist
	 * @return the value of the property
	 */
	public String getString(String name, String defaultValue) {
		return this.configuration.get(name, defaultValue);
	}
	
	/**
	 * Get the value of the "name" property as a boolean.
	 *
	 * @param name the name of the property
	 * @param defaultValue the default value, if the property doesn't exist
	 * @return the value of the property
	 */
	public boolean getBoolean(String name, boolean defaultValue) {
		return this.configuration.getBoolean(name, defaultValue);
	}
	
	/**
	 * Get the value of the "name" property as a Class.
	 *
	 * @param name the name of the property
	 * @param defaultValue the default value, if the property doesn't exist
	 * @return the value of the property
	 */
	public Class<?> getClass(String name, Class<?> defaultValue) {
		return this.configuration.getClass(name, defaultValue);
	}
	
	/**
	 * Get the value of the "name" property as an Enum.
	 *
	 * @param name the name of the property
	 * @param defaultValue the default value, if the property doesn't exist
	 * @return the value of the property
	 */
	public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
		return this.configuration.getEnum(name, defaultValue);
	}
	
	/**
	 * Get the value of the "name" property as a float.
	 *
	 * @param name the name of the property
	 * @param defaultValue the default value, if the property doesn't exist
	 * @return the value of the property
	 */
	public float getFloat(String name, float defaultValue) {
		return this.configuration.getFloat(name, defaultValue);
	}
	
	/**
	 * Get the value of the "name" property as an int.
	 *
	 * @param name the name of the property
	 * @param defaultValue the default value, if the property doesn't exist
	 * @return the value of the property
	 */
	public int getInt(String name, int defaultValue) {
		return this.configuration.getInt(name, defaultValue);
	}
	
	/**
	 * Get the value of the "name" property as an array of int values.
	 *
	 * @param name the name of the property
	 * @return the value of the property
	 */
	public int[] getInts(String name) {
		Collection<String> strings = this.configuration.getStringCollection(name);
		
		int[] result = new int[strings.size()];
		
		int i = 0;
		
		for (String string : strings) {
			result[i] = Integer.parseInt(string);
			i++;
		}

		return result;
	}
	
	/**
	 * Get the value of the "name" property as a long.
	 *
	 * @param name the name of the property
	 * @param defaultValue the default value, if the property doesn't exist
	 * @return the value of the property
	 */
	public long getLong(String name, long defaultValue) {
		return this.configuration.getLong(name, defaultValue);
	}
	
	/**
	 * Get the value of the "name" property as a double.
	 *
	 * @param name the name of the property
	 * @param defaultValue the default value, if the property doesn't exist
	 * @return the value of the property
	 */
	public double getDouble(String name, double defaultValue) {
		String string = this.configuration.get(name);
		
		if (string == null)
			return defaultValue;
		
		return Double.parseDouble(string);
	}
	
	/**
	 * Get the value of the "name" property as a collection of String.
	 *
	 * @param name the name of the property
	 * @return the value of the property
	 */
	public Collection<String> getStringCollection(String name) {
		return this.configuration.getStringCollection(name);
	}
	
	/**
	 * Get the value of the "name" property as an array of String.
	 *
	 * @param name the name of the property
	 * @return the value of the property
	 */
	public String[] getStrings(String name) {
		return this.configuration.getStrings(name);
	}
	
	/**
	 * Set the value of the the "name" property as a String.
	 *
     * @param name the name of the property
	 * @param value the value to set
	 */
	public void setString(String name, String value) {
		this.configuration.set(name, value);
	}
	
	/**
	 * Set the value of the "name" property as a boolean.
	 *
	 * @param name the name of the property
	 * @param value the value to set
	 */
	public void setBoolean(String name, boolean value) {
		this.configuration.setBoolean(name, value);
	}
	
	/**
	 * Set the value of the "name" property as a Class.
	 *
	 * @param name the name of the property
	 * @param value the value to set
	 */
	public void setClass(String name, Class<?> value) {
		this.configuration.setClass(name, value, Object.class);
	}
	
	/**
	 * Set the value of the "name" property as an Enum.
	 *
	 * @param name the name of the property
	 * @param value the value to set
	 */
	public <T extends Enum<T>> void setEnum(String name, T value) {
		this.configuration.setEnum(name, value);
	}
	
	/**
	 * Set the value of the "name" property as a float.
	 *
     * @param name the name of the property
	 * @param value the value to set
	 */
	public void setFloat(String name, float value) {
		this.configuration.setFloat(name, value);
	}
	
	/**
	 * Set the value of the "name" property as an int.
	 *
	 * @param name the name of the property
	 * @param value the value to set
	 */
	public void setInt(String name, int value) {
		this.configuration.setInt(name, value);
	}
	
	/**
	 * Set the value of the "name" property as an array of int values.
	 *
	 * @param name the name of the property
	 * @param values the values to set
	 */
	public void setInts(String name, int... values) {
		String[] stringValues = new String[values.length];
		
		for (int i = 0; i < values.length; i++)
			stringValues[i] = Integer.toString(values[i]);
		
		this.configuration.setStrings(name, stringValues);
	}
	
	/**
	 * Set the value of the "name" property as a long.
	 *
	 * @param name the name of the property
	 * @param value the value to set
	 */
	public void setLong(String name, long value) {
		this.configuration.setLong(name, value);
	}
	
	/**
	 * Set the value of the "name" property as a double.
	 *
	 * @param name the name of the property
	 * @param value the value to set
	 */
	public void setDouble(String name, double value) {
		this.configuration.set(name, Double.toString(value));
	}
	
	/**
	 * Set the value of the "name" property as an array of String.
	 *
	 * @param name the name of the property
	 * @param values the values to set
	 */
	public void setStrings(String name, String... values) {
		this.configuration.setStrings(name, values);
	}
}


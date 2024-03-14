# Twitter Data Analysis with Spark

This Scala application is designed to perform Twitter data analysis using Apache Spark. It consists of two main functionalities: 

1. **Twitter Data Streaming**: Fetches real-time tweets containing specified keywords, and saves them to Hadoop Distributed File System (HDFS) in JSON format.
2. **Twitter Data Analysis**: Reads a CSV file from HDFS, creates a database, and saves the data to HDFS.

## Requirements

- Apache Spark
- Hadoop Distributed File System (HDFS)
- Twitter Developer Account (for API access)

## Usage

### 1. Setting up Twitter Developer Account

Before running the application, you need to obtain Twitter API credentials:

- Consumer Key
- Consumer Secret
- Access Token
- Access Token Secret

### 2. Application Configuration

- **Application Configuration**: Ensure that Spark and HDFS configurations are correctly set in the SparkConf object.
- **Twitter API Configuration**: Replace placeholders for Twitter API credentials with your actual credentials.

### 3. Running the Application

#### Part 1: Twitter Data Streaming

Run the `twitterstreaming` application to stream tweets containing specific keywords and save them to HDFS.

```bash
$ spark-submit --class com.snb.app.twitterstreaming <path_to_jar_file>
```

#### Part 2: Twitter Data Analysis

Run the `twitterAnalysis` application to analyze the Twitter data stored in HDFS.

```bash
$ spark-submit --class com.snb.app.twitterAnalysis <path_to_jar_file>
```

## Files and Directories

- **application.scala**: Contains the Scala code for Twitter data streaming and analysis.
- **/venky/test.csv**: Sample CSV file used for analysis.
- **/tweets**: Directory where streamed tweets are saved in JSON format.

## Additional Notes

- Ensure that HDFS is running and accessible before executing the application.
- Make sure to have necessary permissions to read from and write to HDFS.


---

Feel free to adjust the content according to your specific project details and preferences.

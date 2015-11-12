package com.stratio.cassandra.lucene.util;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Ran Tavory (rantav@gmail.com)
 */
public class CassandraServer {

    private static Logger log = LoggerFactory.getLogger(CassandraServer.class);

    private final String yamlFilePath;
    static CassandraDaemon cassandraDaemon;

    public CassandraServer() {
        this("/cassandra.yaml");
    }

    public CassandraServer(String yamlFile) {
        this.yamlFilePath = yamlFile;
    }

    static ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    public void setup() throws TTransportException, IOException,
                               InterruptedException, ConfigurationException {

        // Create temp dir to store data
        File dir = Files.createTempDir();
        String dirPath = dir.getAbsolutePath();
        log.info("Storing Cassandra files in " + dirPath);

        URL url = Resources.getResource("cassandra.yaml");
        String yaml = Resources.toString(url, Charsets.UTF_8);
        yaml = yaml.replaceAll("REPLACEDIR", dirPath);
        String yamlPath = dirPath + File.separatorChar + "cassandra.yaml";
        PrintWriter printWriter = new PrintWriter(yamlPath);
        printWriter.print(yaml);
        printWriter.flush();
        printWriter.close();

        // make a tmp dir and copy cassandra.yaml and log4j.properties to it
        copy("/logback.xml", dirPath);
        System.setProperty("cassandra.config", "file:" + dirPath + yamlFilePath);
        System.setProperty("log4j.configuration", "file:" + dirPath + "/logback.xml");
        System.setProperty("cassandra-foreground", "true");

        cleanupAndLeaveDirs();
        log.info("Starting executor");

        executor.execute(new CassandraRunner());
        log.info("Started executor");
        try {
            log.info("Sleeping while Cassandra starts --------------------------------------");
            TimeUnit.SECONDS.sleep(30);
            log.info("Done sleeping --------------------------------------------------------");
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }


    public static void teardown() {
        //if ( cassandraDaemon != null )
        //cassandraDaemon.stop();
        executor.shutdown();
        executor.shutdownNow();
        log.info("Teardown complete");
    }

    private static void rmdir(String dir) throws IOException {
        File dirFile = new File(dir);
        if (dirFile.exists()) {
            FileUtils.deleteRecursive(new File(dir));
        }
    }

    /**
     * Copies a resource from within the jar to a directory.
     *
     * @param resource
     * @param directory
     * @throws IOException
     */
    private static void copy(String resource, String directory)
            throws IOException {
        mkdir(directory);
        InputStream is = CassandraServer.class.getResourceAsStream(resource);
        String fileName = resource.substring(resource.lastIndexOf("/") + 1);
        File file = new File(directory + System.getProperty("file.separator") + fileName);
        OutputStream out = new FileOutputStream(file);
        byte buf[] = new byte[1024];
        int len;
        while ((len = is.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        out.close();
        is.close();
    }

    /**
     * Creates a directory
     *
     * @param dir
     * @throws IOException
     */
    private static void mkdir(String dir) throws IOException {
        FileUtils.createDirectory(dir);
    }


    private static void cleanupAndLeaveDirs() throws IOException {
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.resetUnsafe(); // cleanup screws w/ CommitLog, this brings it back to safe state
    }

    public static void cleanup() throws IOException {
        // clean up commitlog
        String[] directoryNames = {DatabaseDescriptor.getCommitLogLocation(),};
        for (String dirName : directoryNames) {
            File dir = new File(dirName);
            if (!dir.exists()) {
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }
            FileUtils.deleteRecursive(dir);
        }

        // clean up data directory which are stored as data directory/table/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations()) {
            File dir = new File(dirName);
            if (!dir.exists()) {
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            }
            FileUtils.deleteRecursive(dir);
        }
    }

    private static void mkdirs() {
        DatabaseDescriptor.createAllDirectories();
    }


    private class CassandraRunner implements Runnable {

        @Override
        public void run() {
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.activate();

        }

    }
}


/***********************************************************************
 	hadoop-gpu
	Authors: Koichi Shirahata, Hitoshi Sato, Satoshi Matsuoka

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: SubmitterToAccels.java
Version: 0.20.1
***********************************************************************/

package org.apache.hadoop.mapred.pipes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;

import javax.xml.soap.Text;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SubmitterToAccels extends Submitter {
	
  /**
   * Set the configuration, if it doesn't already have a value for the given
   * key.
   * @param conf the configuration to modify
   * @param key the key to set
   * @param value the new "default" value to set
   */
  private static void setIfUnset(JobConf conf, String key, String value) {
    if (conf.get(key) == null) {
      conf.set(key, value);
    }
  }
	
	/**
	 * Get the URI of the application's cpu executable.
	 * @param conf
	 * @return the URI where the application's cpu executable is located
	 */
	public static String getCPUExecutable(JobConf conf) {
		return conf.get("hadoop.pipes.executable");
	}
	
	/**
	 * Set the URI for the application's cpu executable. Normally this is a hdfs:
	 * location 
	 * @param conf
	 * @param executable The URI of the application's cpu executable.
	 */
	public static void setCPUExecutable(JobConf conf, String executable) {
		conf.set("hadoop.pipes.executable", executable);
	}
	
	/**
	 * Get the URI of the application's gpu executable.
	 * @param conf
	 * @return the URI where the application's gpu executable is located
	 */
	public static String getGPUExecutable(JobConf conf) {
		/** FIXME **/
		return conf.get("hadoop.accels.gpu.executable");
	}
	
	@Deprecated
	public static RunningJob submitJob(JobConf conf) throws IOException {
		return runJob(conf);
	}
	
	@Deprecated
	public static RunningJob runJob(JobConf conf) throws IOException {
		setupPipesJob(conf);
		return JobClient.runJob(conf);
	}
	
	public static RunningJob jobSubmit(JobConf conf) throws IOException {
		setupPipesJob(conf);
		return new JobClient(conf).submitJob(conf);
	}
	
  private static void setupPipesJob(JobConf conf) throws IOException {
    // default map output types to Text
    if (!getIsJavaMapper(conf)) {
      conf.setMapRunnerClass(PipesMapRunner.class);
      conf.setGPUMapRunnerClass(PipesMapRunner.class);
      // Save the user's partitioner and hook in our's.
      setJavaPartitioner(conf, conf.getPartitionerClass());
      conf.setPartitionerClass(PipesPartitioner.class);
    }
    if (!getIsJavaReducer(conf)) {
      conf.setReducerClass(PipesReducer.class);
      if (!getIsJavaRecordWriter(conf)) {
        conf.setOutputFormat(NullOutputFormat.class);
      }
    }
    String textClassname = Text.class.getName();
    setIfUnset(conf, "mapred.mapoutput.key.class", textClassname);
    setIfUnset(conf, "mapred.mapoutput.value.class", textClassname);
    setIfUnset(conf, "mapred.output.key.class", textClassname);
    setIfUnset(conf, "mapred.output.value.class", textClassname);
    
    // Use PipesNonJavaInputFormat if necessary to handle progress reporting
    // from C++ RecordReaders ...
    if (!getIsJavaRecordReader(conf) && !getIsJavaMapper(conf)) {
      conf.setClass("mapred.pipes.user.inputformat", 
                    conf.getInputFormat().getClass(), InputFormat.class);
      conf.setInputFormat(PipesNonJavaInputFormat.class);
    }
    
    String cpubin = getCPUExecutable(conf);
    String gpubin = getGPUExecutable(conf);
    if (cpubin == null || gpubin == null) {
      throw new IllegalArgumentException("No application program defined.");
    }
    // add default debug script only when executable is expressed as
    // <path>#<executable>
    if (cpubin.contains("#")) {
      DistributedCache.createSymlink(conf);
      // set default gdb commands for map and reduce task 
      String defScript = "$HADOOP_HOME/src/c++/pipes/debug/pipes-default-script";
      setIfUnset(conf,"mapred.map.task.debug.script",defScript);
      setIfUnset(conf,"mapred.reduce.task.debug.script",defScript);
    }
    
    if (gpubin.contains("#")) {
    	DistributedCache.createSymlink(conf);
      // set default gdb commands for map and reduce task 
      String defScript = "$HADOOP_HOME/src/c++/pipes/debug/pipes-default-script";
      setIfUnset(conf,"mapred.map.task.debug.script",defScript);
      setIfUnset(conf,"mapred.reduce.task.debug.script",defScript);
    }
    
    URI[] fileCache = DistributedCache.getCacheFiles(conf);
    if (fileCache == null) {
      fileCache = new URI[2];
    } else {
      URI[] tmp = new URI[fileCache.length+1];
      System.arraycopy(fileCache, 0, tmp, 1, fileCache.length);
      fileCache = tmp;
    }
    try {
      fileCache[0] = new URI(cpubin);
    } catch (URISyntaxException e) {
      IOException ie = new IOException("Problem parsing execable URI " + cpubin);
      ie.initCause(e);
      throw ie;
    }
    try {
    	fileCache[1] = new URI(gpubin);
    } catch (URISyntaxException e) {
    	IOException ie = new IOException("Problem parsing execable URI " + gpubin);
    	ie.initCause(e);
    	throw ie;
    }
    DistributedCache.setCacheFiles(fileCache, conf);
  }
	
	/**
	 * Set the URI for the application's gpu executable. Normally this is a hdfs:
	 * location 
	 * @param conf
	 * @param executable The URI of the application's gpu executable.
	 */
	public static void setGPUExecutable(JobConf conf, String executable) {
		/** FIXME **/
		conf.set("hadoop.accels.gpu.executable", executable);
	}

	
	 static class CommandLineParser {
		private Options options = new Options();
		
		void addOption(String longName, boolean required, String description,
				String paramName) {
			Option option = OptionBuilder.withArgName(paramName).hasArgs(1).withDescription(description).isRequired(required).create(longName);
			options.addOption(option);
		}
		
		void addArgument(String name, boolean required, String description) {
			Option option = OptionBuilder.withArgName(name).hasArgs(1).withDescription(description).isRequired(required).create();
			options.addOption(option);
		}
		
		Parser createParser() {
			Parser result = new BasicParser();
			return result;
		}
		
		void printUsage() {
		}
	}

	/**
	 * Submit an accels job on the command line arguments. 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = new SubmitterToAccels().run(args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		CommandLineParser cli = new CommandLineParser();
		if (args.length == 0) {
			cli.printUsage();
			return 1;
		}
		
		cli.addOption("input", false, "input path to the maps", "path");
		cli.addOption("output", false, "output path from the reduces", "path");
		
		cli.addOption("cpubin", false, "URI to application cpu executable", "class");
		cli.addOption("gpubin", false, "URI to application gpu executable", "class");
		
		Parser parser = cli.createParser();
		try {
			GenericOptionsParser genericParser = new GenericOptionsParser(getConf(), args);
			CommandLine results = 
				parser.parse(cli.options, genericParser.getRemainingArgs());
			JobConf job = new JobConf(getConf());
			
			if (results.hasOption("input")) {
				FileInputFormat.setInputPaths(job, (String) results.getOptionValue("input"));
			}
	        if (results.hasOption("output")) {
	        	FileOutputFormat.setOutputPath(job, new Path((String) results.getOptionValue("output")));
	        }
	        if (results.hasOption("cpubin")) {
	        	setCPUExecutable(job, (String) results.getOptionValue("cpubin"));
	        }
	        if (results.hasOption("gpubin")) {
	        	setGPUExecutable(job, (String) results.getOptionValue("gpubin"));
	        }
	        // if they gave us a jar file, include it into the class path
	        String jarFile = job.getJar();
	        if (jarFile != null) {
	        	final URL[] urls = new URL[] { FileSystem.getLocal(job).
	        			pathToFile(new Path(jarFile)).toURL() };
	            //FindBugs complains that creating a URLClassLoader should be
	            //in a doPrivileged() block. 
	            ClassLoader loader =
	                AccessController.doPrivileged(
	                    new PrivilegedAction<ClassLoader>() {
	                      public ClassLoader run() {
	                        return new URLClassLoader(urls);
	                      }
	                    }
	                  );
	              job.setClassLoader(loader);
	        }
	        runJob(job);
	        return 0;
		} catch (ParseException pe) {
			LOG.info("Error :" + pe);
			cli.printUsage();
			return 1;
		}
	}
}

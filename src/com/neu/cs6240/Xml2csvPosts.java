/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.neu.cs6240;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * This class provides functionality to transform stack-overflow data dump's xml
 * to csv format using Apache Hadoop
 * 
 * @author vishrut
 * 
 */
public class Xml2csvPosts {

	// Mapper class that reads line as a individual row
	public static class PostsMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		private DocumentBuilderFactory factory = null;
		private DocumentBuilder builder = null;
		private Document doc = null;

		/**
		 * setup will be called once per Map Task before any of Map function
		 * call, we'll initialize XML parser here
		 */
		protected void setup(Context context) {
			this.factory = DocumentBuilderFactory.newInstance();
			try {
				this.builder = factory.newDocumentBuilder();
			} catch (ParserConfigurationException e) {
				// ParserConfigurationException => exit
				System.exit(1);
			}
		}

		public void map(LongWritable offset, Text value, Context context)
				throws IOException, InterruptedException {

			InputSource is = new InputSource(new StringReader(value.toString()));

			try {
				this.doc = this.builder.parse(is);
			} catch (SAXException e) {
				// Unable to parse => ignore row
				return;
			}

			// parse xml row and emit only required fields
			Post postObj = new Post(this.doc);

			IntWritable key = new IntWritable();
			key.set(Integer.parseInt(postObj.getId().toString()));

			context.write(key, new Text(postObj.toCsv()));
		}
	}

	// writes the data on the HDFS
	public static class PostsReducer extends
			Reducer<IntWritable, Text, NullWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	// Paritioner for scalability
	public static class PostsPartitioner extends
			Partitioner<IntWritable, Text> {
		/**
		 * Based on the configured number of reducer, this will partition the
		 * data approximately evenly based on number of unique post ids
		 */
		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Setting up the xml tag configurator for splitter
		conf.set("xmlinput.start", "<row ");
		conf.set("xmlinput.end", " />");

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Xml2csvPosts <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Converts Posts.xml to .csv");
		job.setJarByClass(Xml2csvPosts.class);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(PostsMapper.class);
		job.setReducerClass(PostsReducer.class);
		job.setPartitionerClass(PostsPartitioner.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		// Set as per your file size
		job.setNumReduceTasks(15);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
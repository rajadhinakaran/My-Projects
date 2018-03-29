package edu.zigsaw
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
/**
	 * The main application class. 
	 * 
	 *  
	 */

	public static class CRMMapper extends
			Mapper<Object, Text, Text, Text> {
     /**
     * The CRMMapper is created for processing the CRM data . 
     * 
     */
     
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
        /**
     * The `map` function. It receives a line of input from the CRM file, 
     * extracts `user id` and `Sales price` from it, which becomes
     * the output. The output key is `user id` and the output value is 
     * `Sales price`.
     * @param key - Input key - The line offset in the file - ignored.
     * @param value - Input Value - This is the line itself.
     * @param context - Provides access to the OutputCollector and Reporter.
     * @throws IOException
     * @throws InterruptedException 
     */
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("CRM\t" + parts[1]));
		}
	}

	public static class ERPMapper extends
  
    /**
     * The ERPMapper is created for processing the ERP data for it. 
     * 
     */
			Mapper<Object, Text, Text, Text> {
      /** 
     * The `Mapper` function. It receives a line of input from the ERP file, 
     * extracts `customer id` and `customer name` from it, which becomes
     * the output. The output key is `user id` and the output value is 
     * `CRM sales price`.
     * @param key - Input key - The line offset in the file - ignored.
     * @param value - Input Value - This is the line itself.
     * @param context - Provides access to the OutputCollector and Reporter.
     * @throws IOException
     * @throws InterruptedException 
     */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[2]), new Text("ERP\t" + parts[3]));
		}
	}

	public static class ReduceJoinReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String name = "";
			double total = 0.0;
			int count = 0;
			for (Text t : values) {
				String parts[] = t.toString().split("\t");
				if (parts[0].equals("ERP")) {
					count++;
					total += Float.parseFloat(parts[1]);
				} else if (parts[0].equals("CRM")) {
					name = parts[1];
				}
			}
			String str = String.format("%d\t%f", count, total);
			context.write(new Text(name), new Text(str));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Reduce-side join");
		job.setJarByClass(ReduceJoin.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

     /*
      Here , we are going to use MultipleInputs to include multiple mappers for different input files. 
     */
	
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CRMMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, ERPMapper.class);
		Path outputPath = new Path(args[2]);		
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

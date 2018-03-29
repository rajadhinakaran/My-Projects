package com.hadoop;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class QuakeInfo extends Configured implements Tool{

public static class QIMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
private String[] str;
private float temperature;
private String country;

public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{

/* Input value is split based on 'tab', which is provided in the input file.*/
str = value.toString().split("\t"); 

temperature = Float.parseFloat(str[6]); //sixth column is temperature
country = str[9]; //9th column is country

ctx.write(new Text(country), new FloatWritable(temperature));
}
}

public static class QIReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{

public void reduce(Text key, Iterable <FloatWritable> value, Context ctx) throws IOException, InterruptedException{

float max = 0;
float tmp = 0;

/* After iterating, max temperature is calculated*/
Iterator<FloatWritable> val = value.iterator();
while(val.hasNext()){
tmp = val.next().get();
if(max < tmp)
max = tmp;
}
ctx.write(key, new FloatWritable(max));
}
}

@Override
public int run(String[] args) throws Exception {
// TODO Auto-generated method stub
Configuration conf = this.getConf();

Job job = new Job(conf,"Quake Info");
job.setJarByClass(QuakeInfo.class);

job.setInputFormatClass(TextInputFormat.class);
TextInputFormat.addInputPath(job, new Path(args[0]));

job.setOutputFormatClass(TextOutputFormat.class);
TextOutputFormat.setOutputPath(job, new Path(args[1]));

job.setMapperClass(QIMapper.class);
job.setReducerClass(QIReducer.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(FloatWritable.class);

boolean res = job.waitForCompletion(true);
if(res)
return 0;
else
return -1;
}



public static void main(String args[]) throws Exception{
int res = ToolRunner.run(new Configuration(), new QuakeInfo(), args);
System.exit(res);
}
}
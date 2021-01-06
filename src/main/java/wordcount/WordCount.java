/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author Solechoel Arifin
 */
public class WordCount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
       Configuration conf = new Configuration();
       String[] otherArgs = args;
       
       if (otherArgs.length != 2) {
         System.err.println("Usage: WordCount <input> <output>");
         System.exit(2);
       }
       Job job = new Job(conf, "wordcount");
       job.setJarByClass(WordCount.class);
       
       job.setMapperClass(WordCountMapper.class);
       job.setReducerClass(WordCountReducer.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(IntWritable.class);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       FileSystem fs = FileSystem.get(conf);

      
       fs.delete(new Path(otherArgs[1]), true);
       
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
       
       return (job.waitForCompletion(true)? 0 : 1);
       
  }
  
  public static void main(String[] args) throws Exception{
      int exitCode=ToolRunner.run(new WordCount(), args);
      System.exit(exitCode);
  }

}

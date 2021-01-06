/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datatype;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Solechoel Arifin
 *
 */


public class Calculation extends Configured implements Tool{
    public int run(String[] args) throws Exception {
       Configuration conf = new Configuration();
       String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
       if (otherArgs.length != 2) {
          System.out.println("Usage: Caculation <input> <output>");
          ToolRunner.printGenericCommandUsage(System.out);
          System.exit(1);
       }
       Job job = new Job(conf, "Calculation");
       job.setJarByClass(this.getClass());
       
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
       
       job.setMapperClass(CalculationMapper.class);
       job.setReducerClass(CalculationReducer.class);

       job.setMapOutputKeyClass(CitySales.class);
       job.setMapOutputValueClass(DoublePair.class);
       
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
       
       FileSystem fs = FileSystem.get(conf);
       fs.delete(new Path(otherArgs[1]), true);
       
       job.waitForCompletion(true);
       return 0;   
  }
  public static void main(String[] args) throws Exception{
      int exitCode=ToolRunner.run(new Calculation(), args);
      System.exit(exitCode);
  }
}




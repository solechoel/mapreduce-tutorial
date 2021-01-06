/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package outputformat;

import inputformat.MyOrderMapper;
import inputformat.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import datatype.DoublePair;
import org.apache.hadoop.fs.FileSystem;

public class MyOrderDriver extends Configured implements Tool{
    public int run(String[] args) throws Exception {
       Configuration conf = new Configuration();
       String[] otherArgs = args;
       if (otherArgs.length != 2) {
          System.out.println("Usage: MyOrderDriver <input> <output>");
          ToolRunner.printGenericCommandUsage(System.out);
          System.exit(1);
       }
       Job job = new Job(conf, "My OrderDriver");
       job.setJarByClass(this.getClass());
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

       job.setInputFormatClass(OrderInputFormat.class);
       job.setOutputFormatClass(XMLOutputFormat.class);
       
       job.setMapperClass(MyOrderMapper.class);

       job.setReducerClass(MyOrderReducer2.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(DoublePair.class);
       
       job.setOutputKeyClass(Text.class);
       //berubah
       job.setOutputValueClass(DoublePair.class);
       
       FileSystem fs = FileSystem.get(conf);
       fs.delete(new Path(otherArgs[1]), true);
       
       job.waitForCompletion(true);
       return 0;   
  }
  public static void main(String[] args) throws Exception{
      int exitCode=ToolRunner.run(new MyOrderDriver(), args);
      System.exit(exitCode);
  }
}




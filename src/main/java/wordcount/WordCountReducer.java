/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wordcount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 *
 * @author Solechoel Arifin
 */
public class WordCountReducer extends Reducer<Text,IntWritable,
                                              Text,IntWritable> {
      
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
            int sum=0;
           
            for (IntWritable val : values) {
                
                sum=sum+val.get();
            }
            
            context.write(key, new IntWritable(sum));
    }
  }

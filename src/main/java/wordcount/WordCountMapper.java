/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wordcount;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Solechoel Arifin
 */
public class WordCountMapper extends Mapper<LongWritable, Text, 
                                            Text, IntWritable>{

    public void map(LongWritable key, Text value, 
                    Context context
                    ) throws IOException, InterruptedException {
        
        
        
        String line=value.toString();
        
        for (String word: line.split("\\W+")){
            if (word.length()>1){
                context.write(new Text(word), new IntWritable(1));
            }
        }
       
        
    }
  }

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package inputformat;


import datatype.OrderWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author Solechoel Arifin
 *
 */


public class OrderInputFormat extends
             FileInputFormat<LongWritable, OrderWritable>{
    
    @Override
    public RecordReader<LongWritable,OrderWritable> createRecordReader(
                       InputSplit is, TaskAttemptContext tac)  {
        return new OrderRecordReader();
    }
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package inputformat;

import java.io.IOException;

import datatype.OrderWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 *
 * @author Solechoel Arifin
 *
 */


public class OrderRecordReader extends RecordReader<LongWritable, OrderWritable>{

    private LineRecordReader reader;
    private OrderWritable value;
    public OrderRecordReader(){
         this.reader=new LineRecordReader();
    }
    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac)
                                  throws IOException, InterruptedException {
            reader.initialize(is, tac);
    }
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
         return reader.getCurrentKey();
    }
    @Override
    public OrderWritable getCurrentValue() throws IOException, InterruptedException {
         return value;
    }
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
    @Override
    public void close() throws IOException {
        reader.close();
   }

   @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.nextKeyValue()){
            String line=reader.getCurrentValue().toString();
            String[] v_array=line.split(",");
            value=new OrderWritable();
            value.setDate(v_array[0]);
            value.setCity(v_array[1]);
            value.setSales(v_array[2]);
            value.setItem(v_array[3]);
            value.setUnit(Integer.parseInt(v_array[4]));
            value.setUnitPrice(Double.parseDouble(v_array[5]));
            return true;
        }else{
            return false;
        }
    }

}

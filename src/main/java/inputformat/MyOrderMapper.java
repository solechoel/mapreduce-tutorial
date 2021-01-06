/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inputformat;

import java.io.IOException;

import datatype.DoublePair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Solechoel Arifin
 *
 */

public class MyOrderMapper extends Mapper<LongWritable, Text, Text, DoublePair> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split(",");
        String city=fields[1];
        double unit=Double.parseDouble(fields[4]);
        double unit_price=Double.parseDouble(fields[5]);
        context.write(new Text(city), new DoublePair(unit,unit*unit_price));
        
    }
    
}

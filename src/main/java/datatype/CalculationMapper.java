/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datatype;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Solechoel Arifin
 *
 */

public class CalculationMapper extends Mapper<LongWritable, Text, CitySales, DoublePair> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] fields=line.split(",");
        String city=fields[1];
        String sales=fields[2];
        try {
            double unit = Double.parseDouble(fields[4]);
            double unit_price = Double.parseDouble(fields[5]);
            context.write(new CitySales(city, sales), new DoublePair(unit, unit * unit_price));
        }catch (Exception e){}
        
    }
    
}

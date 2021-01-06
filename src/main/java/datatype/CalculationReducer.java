/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datatype;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Solechoel Arifin
 *
 */

public class CalculationReducer extends Reducer<CitySales,DoublePair,
                 Text,Text>{

    @Override
    protected void reduce(CitySales key, Iterable<DoublePair> values, Context context) throws IOException, InterruptedException {
        double sum_unit=0;
        double sum_price=0;
        for (DoublePair val:values){
            sum_unit=sum_unit+val.getFirst();
            sum_price=sum_price+val.getSecond();
        }
        String result=sum_unit+","+sum_price;
        String aKey=key.getCity()+","+key.getSales();
        context.write(new Text(aKey), new Text(result));
    }
    
}

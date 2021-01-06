/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inputformat;

import java.io.IOException;

import datatype.DoublePair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Solechoel Arifin
 *
 */

public class MyOrderReducer extends Reducer<Text, DoublePair,
                 Text,Text>{

    @Override
    protected void reduce(Text key, Iterable<DoublePair> values, Context context) throws IOException, InterruptedException {
        double sum_unit=0;
        double sum_price=0;
        for (DoublePair val:values){
            sum_unit=sum_unit+val.getFirst();
            sum_price=sum_price+val.getSecond();
        }
        String result=sum_unit+","+sum_price;
        context.write(key, new Text(result));
    }
    
}

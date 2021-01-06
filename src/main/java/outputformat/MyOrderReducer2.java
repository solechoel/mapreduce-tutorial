/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package outputformat;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import datatype.DoublePair;

public class MyOrderReducer2 extends Reducer<Text,DoublePair,Text,DoublePair>{

  @Override
  public void reduce(Text key, Iterable<DoublePair> values, Context context)
              throws IOException, InterruptedException {

      double sum_unit = 0.0;
      double sum_price = 0.0;
      for (DoublePair val : values) {
          sum_unit = sum_unit + val.getFirst();
          sum_price= sum_price + val.getSecond();
      }
      DoublePair dp=new DoublePair(sum_unit,sum_price);
      context.write(key, dp);
   }
}


/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
/*
 Author : Solechoel Arifin
 */

public class DoublePair implements Writable{

    double first;
    double second;

    public DoublePair(){}

    public DoublePair(double first, double second){
      this.first=first;
      this.second=second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
       out.writeDouble(first);
       out.writeDouble(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      first = in.readDouble();
      second = in.readDouble();
    }

    public double getFirst() {
        return first;
    }

    public void setFirst(double first) {
        this.first = first;
    }

    public double getSecond() {
        return second;
    }

    public void setSecond(double second) {
        this.second = second;
    }


}

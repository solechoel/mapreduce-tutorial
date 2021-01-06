package datatype;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author Solechoel Arifin
 *
 */


public class CitySales implements WritableComparable<CitySales> {
    String city="";
    String sales="";

    public CitySales() {
    }

    public CitySales(String city, String sales) {
        this.city = city;
        this.sales = sales;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getSales() {
        return sales;
    }

    public void setSales(String sales) {
        this.sales = sales;
    }

    @Override
    public int compareTo(CitySales other) {
        int cmp=city.compareTo(other.city);
        if (cmp!=0){
            return cmp;
        }
        return sales.compareTo(other.sales);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
         dataOutput.writeUTF(city);
         dataOutput.writeUTF(sales);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        city=dataInput.readUTF();
        sales=dataInput.readUTF();
    }

    @Override
    public int hashCode() {
        return (city+sales).hashCode();
    }
}

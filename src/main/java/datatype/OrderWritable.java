package datatype;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author Solechoel Arifin
 *
 */

public class OrderWritable implements Writable {
    String date;
    String city;
    String sales;
    String item;
    int unit;
    double unitPrice;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(city);
        dataOutput.writeUTF(sales);
        dataOutput.writeUTF(item);
        dataOutput.writeInt(unit);
        dataOutput.writeDouble(unitPrice);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        date=dataInput.readUTF();
        city=dataInput.readUTF();
        sales=dataInput.readUTF();
        item=dataInput.readUTF();
        unit=dataInput.readInt();
        unitPrice=dataInput.readDouble();
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
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

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public int getUnit() {
        return unit;
    }

    public void setUnit(int unit) {
        this.unit = unit;
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(double unitPrice) {
        this.unitPrice = unitPrice;
    }
}

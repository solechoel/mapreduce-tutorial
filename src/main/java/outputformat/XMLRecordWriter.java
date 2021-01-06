/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package outputformat;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import datatype.DoublePair;

public class XMLRecordWriter<K,V> extends RecordWriter<K, V>{

    private DataOutputStream out;
    public XMLRecordWriter(DataOutputStream out) throws IOException {
       this.out = out;
       out.writeBytes("<results>\n");
    }
    @Override
    public synchronized void write(K key, V value) throws IOException {
       out.writeBytes("  <data>\n");
       out.writeBytes("     <city>"+key.toString()+"</city>\n");
       if (value instanceof DoublePair){
           DoublePair dp=(DoublePair)value;
           out.writeBytes("     <total_unit>"+dp.getFirst()+"</total_unit>\n");
           out.writeBytes("     <total_price>"+dp.getSecond()+"</total_price>\n");
       }
       out.writeBytes("  </data>\n");
    }
    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
       try {
           out.writeBytes("</results>\n");
       } finally {
           out.close();
       }
    }
}

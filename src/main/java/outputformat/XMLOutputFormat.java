/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package outputformat;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class XMLOutputFormat<K,V>  extends FileOutputFormat<K,V>{
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
                   throws IOException, InterruptedException{
         String extension="";
         Path file = this.getDefaultWorkFile(job,extension);
         
         FileSystem fs = file.getFileSystem(job.getConfiguration());
         FSDataOutputStream fileOut = fs.create(file);

         return new XMLRecordWriter<K, V>(fileOut);
    }
}



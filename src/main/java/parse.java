import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.io.FileUtils;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

public class parse {
    public static void main(String[] args) throws IOException {
        // test program
        String root = "hdfs://localhost:9000/";
        Configuration config = new Configuration();
        Path extract_path = new Path(root + "h5_data");

        // configure Hadoop file system and set file path
        FileSystem hdfs = FileSystem.get(URI.create(root), config);
        if(!hdfs.exists(extract_path))
            if (!hdfs.mkdirs(extract_path))
                System.err.println("mkdir " + extract_path + " failed!");

        // set input path
        SeekableInput input = new FsInput(new Path(root + "A.avro"), config);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
        GenericData.Record record = new GenericData.Record(fileReader.getSchema());

        while (fileReader.hasNext()) {
            fileReader.next(record);
            String filePathName = record.get(0).toString();
            FSDataOutputStream outputStream = hdfs.create(new Path(extract_path + filePathName));

            // get file content
            byte[] content = new byte[((ByteBuffer)record.get(1)).remaining()];
            ((ByteBuffer)record.get(1)).get(content);

            // write to hdfs
            outputStream.write(content);
            outputStream.close();

            // check consistency
            String sha = DigestUtils.shaHex(content).toUpperCase();
            if (!record.get(2).toString().equals(sha))
                System.err.println(record.get(0).toString() + " content inconsistent!");
        }
        fileReader.close(); // also closes underlying FsInput
        hdfs.close();
    }
}

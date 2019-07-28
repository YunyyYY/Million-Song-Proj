import org.apache.hadoop.fs.*;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.nio.ByteBuffer;

public class parse {
    public static void main(String[] args) throws Exception {

        // configure Hadoop file system
        String root = "hdfs://localhost:9000/";
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(root), config);
        Path extract_path = new Path(root + "h5_data");

        // acquire input file list and create output file path
        RemoteIterator avro_list = hdfs.listFiles(new Path(root), true);
        if(!hdfs.exists(extract_path)) {
            if (!hdfs.mkdirs(extract_path))
                System.err.println("mkdir " + extract_path + " failed!");
        }

        while (avro_list.hasNext()) {
            LocatedFileStatus av = (LocatedFileStatus)avro_list.next();
            System.out.println(av.getPath());
            if (av.getPath().equals(extract_path))
                continue;               // in case output path already exist

            // set input path
            SeekableInput input = new FsInput(av.getPath(), config);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>();
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
            GenericData.Record record = new GenericData.Record(fileReader.getSchema());

            while (fileReader.hasNext()) {
                fileReader.next(record);
                String filePathName = root + "h5_data" + record.get(0).toString();
                FSDataOutputStream outputStream = hdfs.create(new Path(filePathName));

                // get file content
                byte[] content = new byte[((ByteBuffer)record.get(1)).remaining()];
                ((ByteBuffer)record.get(1)).get(content);

                // check consistency
                String sha = DigestUtils.shaHex(content).toUpperCase();
                if (!record.get(2).toString().equals(sha))
                    System.err.println(record.get(0).toString() + " corrupted!");
                else {  // write to hdfs
                    outputStream.write(content);
                    outputStream.close();
                }
            }
            fileReader.close(); // also closes underlying FsInput
        }
        hdfs.close();
    }
}

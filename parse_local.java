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

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;

public class parse_local {
    public static void main(String[] args) throws Exception {

        // specify which letter to download
        System.out.print("Specify letter:");
        char file_letter = (char)System.in.read();
        File folder = new File(Character.toString(file_letter));
        if (!folder.mkdir())
            System.err.println("dir already exists");

        // configure Hadoop file system
        String root = "hdfs://localhost:9000/";
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(root), config);

        // acquire input file list and create output file path
        RemoteIterator avro_list = hdfs.listFiles(new Path(root), true);

        while (avro_list.hasNext()) {
            LocatedFileStatus av = (LocatedFileStatus)avro_list.next();
            System.out.println(av.getPath());

            // set input path
            SeekableInput input = new FsInput(av.getPath(), config);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>();
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
            GenericData.Record record = new GenericData.Record(fileReader.getSchema());

            while (fileReader.hasNext()) {
                fileReader.next(record);
                String filePathName = file_letter + record.get(0).toString();
                File folder_file = new File(filePathName.substring(0, 8));
                if (!folder_file.exists())
                    if (!folder_file.mkdirs())
                        System.err.println("Fail to create " + filePathName.substring(0, 8));

                File file = new File(filePathName);
                if (!file.createNewFile())
                    System.err.println(filePathName + "already exists!");
//
                byte[] content = new byte[((ByteBuffer)record.get(1)).remaining()];
                ((ByteBuffer)record.get(1)).get(content);

                // check consistency
                String sha = DigestUtils.shaHex(content).toUpperCase();
                if (!record.get(2).toString().equals(sha))
                    System.err.println(record.get(0).toString() + " corrupted!");
                else {
                    OutputStream outputStream = new FileOutputStream(filePathName);
                    outputStream.write(content);
                    outputStream.close();
                }
            }
            fileReader.close(); // also closes underlying FsInput
        }
        hdfs.close();
    }
}

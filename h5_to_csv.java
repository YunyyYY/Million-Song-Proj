import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5SimpleWriter;

import java.net.URI;

public class h5_to_csv {
    public static void main(String[] args) throws Exception{
        // convert h5 to csv
        String root = "hdfs://localhost:9000/";
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(root), config);

        String basedir = root + "h5_data/";

//        IHDF5SimpleWriter writer = HDF5Factory.open();
    }
}

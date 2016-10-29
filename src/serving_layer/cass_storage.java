package serving_layer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Dominik on 29.10.2016.
 */
public class cass_storage {

    public static void main(String[] args){
        //connect to hdfs file
        URI uri = URI.create("hdfs://localhost:9000/user/dominik/Asimar/data.txt");
        Configuration config = new Configuration();
        config.addResource("/HADOOP_HOME/conf/core-site.xml");
        config.addResource("/HADOOP_HOME/conf/hdfs-site.xml");
        String data = null;
        try {
            FileSystem file = FileSystem.get(uri,config);

            //init inputstream
            FSDataInputStream in = file.open(new Path(uri));
            data = in.readUTF();

        }catch (IOException ex){ex.printStackTrace();}

        //split the data
        String[] lines = data.split("\n");

        //create cassandra cluster
        Cluster cluster = Cluster.builder().addContactPoint("localhost:2221").build();
        //connect to database
        Session session = cluster.connect("Main_Data");
        //create prepared statement
        PreparedStatement statement = session.prepare("INSERT INTO data (customer,articleNr,gender,age,zip) " +
                "VALUES(?,?,?,?,?)");

        for(String line:lines){
            String[] line_data = line.split(" ");
            //now init the new data
            session.execute(statement.bind(line_data[0],line_data[1],line_data[2],line_data[3],line_data[4]));
        }

        session.close();
        cluster.close();



    }

}

package speed_layer;

import com.datastax.driver.core.*;
import com.datastax.driver.core.PreparedStatement;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import java.util.Map;

/**
 * Created by Dominik on 29.10.2016.
 */
public class CassandraBolt implements IRichBolt {
    OutputCollector collector;
    boolean deleted = false;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        //get the data from the tuple
        Integer articleNr = Integer.parseInt(tuple.getString(0));
        Integer gender = Integer.parseInt(tuple.getString(1));
        Integer age = Integer.parseInt(tuple.getString(2));
        Integer zip = Integer.parseInt(tuple.getString(3));


        //create cassandra cluster
        Cluster cluster = Cluster.builder().addContactPoint("localhost:2221").build();
        //connect to database
        Session session = null;
        if (gender == 1){
            session = cluster.connect("women");
        }else {
            session = cluster.connect("men");
        }


        //first we delete all data since the data are already on the batch layer
        // and available  through the serving layer
        if(!deleted){
            session.execute("DELETE * FROM data");
            deleted = true;
        }


        //create prepared statement
        PreparedStatement statement = session.prepare("INSERT INTO data (articleNr,gender,age,zip) " +
                "VALUES(?,?,?,?)");

        //now init the new data
        session.execute(statement.bind(articleNr,gender,age,zip));

        session.close();
        cluster.close();

    }

    @Override
    public void cleanup() {
        System.out.println("Process finished");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

package Data_Update;

/**
 * Created by Dominik on 25.10.2016.
 */
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class Updater {

    //database information
    private final String userID = "root";
    private final String password = "password";
    private final int port = 3306;
    private final String serverName = "localhost";
    private final String dataBase = "messenger";

    //database variables
    private MysqlDataSource dataSource;
    private Connection connection;
    private Statement statement;

    //get the data from the database saved in a table for sold articles each day
    //We get informations about the article and the age and gender of the customer
    private ArrayList getData(){
        dataSource = new MysqlDataSource();
        dataSource.setDatabaseName(dataBase);
        dataSource.setServerName(serverName);
        dataSource.setPort(port);
        ArrayList list = new ArrayList();

        //connect to the database
        try {
            connection = dataSource.getConnection(userID,password);
            statement = connection.createStatement();
            //execute SQL query and get the data
            ResultSet result = statement.executeQuery("SELECT articleNr,gender,age FROM sold_articles");
            //add data to the list
            while (result.next()){
                String data = result.getString("articleNr") + " " + result.getString("gender") + " " + result.getString("age");
                list.add(data);
            }
            result.close();
            statement.close();
            connection.close();

        }catch (Exception ex){ex.printStackTrace();}

        return list;
    }


    public static void main(String[] args){
        //kafka inforamtion
        String topic = "Asimar";

        //init updater and get the data from the database
        Updater updater = new Updater();
        ArrayList data = updater.getData();

        //init the Properties for kafka consumer producer
        Properties props = new Properties();
        //assign localhost id for the kafka broker/s
        props.put("bootstrap.servers","localhost:9093");
        props.put("acks","all");
        //if the request fails, the producer can atomatically retry
        props.put("retries",0);
        //buffer
        props.put("batch.size",16384);
        //we reduce the number of requests less than 0
        props.put("linger.ms",1);
        //total memory
        props.put("buffer.memory",33554421);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //init kafka producer
        KafkaProducer <String,String> producer = new KafkaProducer<>(props);
        //send messages to the brokers
        for(int i=0; i < data.size(); i++){
            producer.send(new ProducerRecord<String,String>(topic,Integer.toString(i),data.get(i).toString()));
        }
        producer.close();


    }
}

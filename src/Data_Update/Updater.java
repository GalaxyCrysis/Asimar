package Data_Update;

/**
 * Created by Dominik on 25.10.2016.
 */
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class Updater {

    //database information
    private final String userID = "root";
    private final String password = "password";
    private final int port = 3306;
    private final String serverName = "localhost";
    private final String dataBase = "messenger";

    //database variables
    MysqlDataSource dataSource;
    Connection connection;
    Statement statement;

    //get the data from the database saved in a table for sold articles each day
    //We get informations about the article and the age and gender of the customer
    public ArrayList getData(){
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
        Updater updater = new Updater();
        ArrayList data = new ArrayList();
        data = updater.getData();


    }
}

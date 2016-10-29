package speed_layer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Dominik on 28.10.2016.
 */
public class processBolt implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //process the age: If the customer is a female, we return 1, if male we return 0
        Integer gender;
        if(tuple.getString(1).equals("female")){
            gender = 1;
        }else {
            gender = 2;
        }

        //now process the articleNr: For example if the string contains a ED it means it is a electrical device, so
        //we give the number 3
        String articleNr = tuple.getString(0);
        Integer article = null;

        if(articleNr.contains("EE")){
            //EE means it a an entertainment article like console or video games
            article = 1;
        }else if(articleNr.contains("BO")){
            //article is a book
            article = 2;
        }else if(articleNr.contains("ED")){
            //article is an electrical device like bulbs
            article = 3;
        }else if(articleNr.contains("KI")){
            //kitchen article
            article = 4;
        }else if(articleNr.contains("HA")){
            //article ia a home appliance
            article = 5;
        }else if(articleNr.contains("GA")){
            //article is a garden article
            article = 6;
        }else if(articleNr.contains("MA")){
            //article is a machine like washing machine
            article = 7;
        }else if(articleNr.contains("BE")){
            //beauty article
            article = 8;
        }else if (articleNr.contains("CL")){
            //clothing
            article = 9;
        }else if(articleNr.contains("HS")){
            //hardware store
            article = 10;
        }
        else if(articleNr.contains("SP")){
            //sport article
            article = 11;
        }
        Integer age = Integer.parseInt(tuple.getString(2));
        Integer zip = Integer.parseInt(tuple.getString(3));
        //emit the values
        collector.emit(new Values(article,gender,age,zip));
        collector.ack(tuple);

    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("article","gender","age","zip"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

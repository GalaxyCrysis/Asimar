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
public class SplitBolt implements IRichBolt {

    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

    }

    //we split the string into the data we need for further analysis
    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");
        collector.emit(new Values(words[0],words[1],words[2],words[3]));
        collector.ack(tuple);

    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("articleNr","gender","age","zip"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

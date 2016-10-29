package batch_layer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by Dominik on 29.10.2016.
 */
public class MapReduce {
    //Mapper class
   public static class EMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String[] data = text.toString().split(" ");
            String customer = data[0];
            String customerData = data[1] + " " + data[2] + " " + data[3] + " " + data[4];

            //emit the new data
            outputCollector.collect(new Text(customer),new Text(customerData));
        }
    }

    public static class EReducer extends MapReduceBase implements Reducer<Text, Text, Text,Text>{
        @Override
        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String[] data = iterator.toString().split(" ");
            Integer gender = null;
            if(data[1].equals("female")){
                gender = 1;
            }else {
                gender = 2;
            }

            //now process the articleNr: For example if the string contains a ED it means it is a electrical device, so
            //we give the number 3
            String articleNr = data[0];
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
            String new_data = article + " " + gender + " " + data[2] + " " + data[3];
            outputCollector.collect(new Text(text),new Text(new_data));

        }
    }

    public static void main(String[] args){
        //init job
        JobConf conf = new JobConf(MapReduce.class);
        conf.setJobName("Asimar-Job");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(EMapper.class);
        conf.setReducerClass(EReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //specify input and output files
        FileInputFormat.setInputPaths(conf,new Path("/user/dominik/Asimar/new_data.txt"));
        FileOutputFormat.setOutputPath(conf, new Path("/user/dominik/Asimar/data.txt"));

        try {
            JobClient.runJob(conf);
        }catch (IOException ex){ex.printStackTrace();}

    }


}

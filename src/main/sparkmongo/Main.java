package sparkmongo;

//hadoop package
import org.apache.hadoop.conf.Configuration;
//spark package
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//mongo package
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;

import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

public class Main {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        SparkConf sparkConf = new SparkConf().setAppName("SparkMongo");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Configuration mongodbConfig = new Configuration();

        mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");

        mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/marketdata.minibars");

        JavaPairRDD<Object, BSONObject> minBarRawRDD = sc.newAPIHadoopRDD(mongodbConfig, MongoInputFormat.class,
                Object.class, BSONObject.class);

        JavaRDD<BSONObject> minBarRDD = minBarRawRDD.values();

        JavaPairRDD<String, Iterable<BSONObject>> groupedBars = minBarRDD.sortBy(new Function<BSONObject, String>() {

            @Override
            public String call(BSONObject v1) throws Exception {
                // TODO Auto-generated method stub
                return v1.get("Timestamp").toString();
            }

        }, true, 1).groupBy(new Function<BSONObject, String>() {

            @Override
            public String call(BSONObject v1) throws Exception {
                // TODO Auto-generated method stub
                String pattern = "yyyy-mm-dd HH:mm";

                SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
                Date date = sdf.parse(v1.get("Timestamp").toString()); 

                return v1.get("Symbol").toString() + String.valueOf(Math.floor(date.getTime() / (5*60)));
            }
        });
        
        JavaRDD<Tuple2<String, BSONObject>> resultRDD = groupedBars.map(new Function<Tuple2<String,Iterable<BSONObject>>, Tuple2<String, BSONObject>>() {
            
            private int size(Iterable<?> it) {
              if (it instanceof Collection)
                return ((Collection<?>)it).size();

              // else iterate

              int i = 0;
              for (Object obj : it) i++;
              return i;
            }
            
            @Override
            public Tuple2<String, BSONObject> call(Tuple2<String, Iterable<BSONObject>> v1) throws Exception {
                // TODO Auto-generated method stub
                int counter = 0;
                
                BSONObject outputDoc = new BasicBSONObject();
                
                Double low = Double.MAX_VALUE;
                Double high = Double.MIN_VALUE;
                String openTime = null;
                Double openPrice = null;
                
                for(BSONObject doc : v1._2) {
                    if(counter == 0) {
                        openTime = (String) doc.get("Timestamp");
                        openPrice = Double.parseDouble(doc.get("Open").toString());
                    }

                    Double tempLow = Double.parseDouble(doc.get("Low").toString());
                    
                    if(tempLow < low) {
                        low = tempLow;
                    }
                    
                    Double tempHigh = Double.parseDouble(doc.get("High").toString());

                    if(tempHigh > high) {
                        high = tempHigh;
                    }
                    
                    counter = counter + 1;
                    
                    if(counter == this.size(v1._2)) {
                        Double close = Double.parseDouble(doc.get("Close").toString());
                        
                        outputDoc.put("Symbol", v1._1);
                        outputDoc.put("Timestamp", openTime);
                        outputDoc.put("Open", openPrice);
                        outputDoc.put("High", high);
                        outputDoc.put("Low", low);
                        outputDoc.put("Close", close);
                    }
                }
                return new Tuple2<String, BSONObject>(v1._1, outputDoc);
            }
        });

        Configuration outputConfig = new Configuration();

        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/marketdata.fiveminutebars");
        
        JavaPairRDD.fromJavaRDD(resultRDD).saveAsNewAPIHadoopFile(
                "file:///this-is-completely-unused", 
                Object.class,
                BSONObject.class, 
                MongoOutputFormat.class, 
                outputConfig);
        
        sc.close();
    }

}

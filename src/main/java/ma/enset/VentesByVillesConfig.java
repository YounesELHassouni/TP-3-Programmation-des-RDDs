package ma.enset;

import org.apache.spark.SparkConf;

public class VentesByVillesConfig {
    public static SparkConf localConfig(String appName){
        return new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]");
    }
    public static SparkConf remoteConfig(String appName){
        return new SparkConf()
                .setAppName(appName);
    }
}

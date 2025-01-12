package ma.enset;

import org.apache.spark.SparkConf;

public class VentesParVilleMain {
    public static void main(String[] args) {
        //SparkConf conf = VentesByVillesConfig.localConfig("Local Ventes par ville");
        //VentesParVilleJob.run(conf, "ventes.txt");
        SparkConf conf = VentesByVillesConfig.localConfig("Remote Ventes par ville");
        VentesParVilleJob.run(conf, "hdfs://namenode:8020/input/ventes.txt");
    }
}

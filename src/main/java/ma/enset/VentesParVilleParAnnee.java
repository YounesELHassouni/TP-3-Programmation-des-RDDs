package ma.enset;

import org.apache.spark.SparkConf;

public class VentesParVilleParAnnee {
    public static void main(String[] args) {
        int annee =Integer.parseInt(args[0]);
        //SparkConf conf = VentesByVillesConfig.localConfig("Local Ventes par ville Par annee");
        //VentesParVilleJob.run(conf, "ventes.txt",annee);
        SparkConf conf = VentesByVillesConfig.localConfig("Remote Ventes par ville Par annee");
        VentesParVilleJob.run(conf, "hdfs://namenode:8020/input/ventes.txt",annee);

    }
}

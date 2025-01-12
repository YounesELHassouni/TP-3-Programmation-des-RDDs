package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class VentesParVilleJob {
    public static void run(SparkConf conf, String ventesFileLocation){
        try(JavaSparkContext sparkContext = new JavaSparkContext(conf)){
            sparkContext.setLogLevel("WARN");
            sparkContext.textFile(ventesFileLocation)
                    .mapToPair(VentesByVilleProcessing::mapToVillePrixPair)
                    .reduceByKey(Double::sum)
                    .collect()
                    .forEach(VentesByVilleProcessing::logResult);
        }

    }
    public static void run(SparkConf conf, String ventesFileLocation, int annee){
        try(JavaSparkContext sparkContext = new JavaSparkContext(conf)){
            Broadcast<Integer> anneeBroadcast = sparkContext.broadcast(annee);
            sparkContext.setLogLevel("WARN");
            JavaPairRDD<String, Double> ventesParVilleParAnnee =
                    sparkContext.textFile(ventesFileLocation)
                            .filter(line -> VentesByVilleProcessing.filterVentesParAnnee(line,anneeBroadcast))
                            .mapToPair(VentesByVilleProcessing::mapToVillePrixPair)
                            .reduceByKey(Double::sum);

            if (ventesParVilleParAnnee.count()==0)
                System.out.println("Aucune vente pour l'année "+annee);
            else {
                System.out.println("Ventes par ville pour l'année "+annee);
                ventesParVilleParAnnee.collect()
                        .forEach(VentesByVilleProcessing::logResult);
            }
        }
    }
}

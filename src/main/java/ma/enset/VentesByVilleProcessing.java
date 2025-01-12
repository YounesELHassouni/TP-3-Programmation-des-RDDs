package ma.enset;

import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class VentesByVilleProcessing {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    public static Tuple2<String,Double> mapToVillePrixPair(String line){
        String[] cols = line.split(" ");
        return new Tuple2<>(cols[1], Double.parseDouble(cols[3]));
    }
    public static boolean filterVentesParAnnee(String line, Broadcast<Integer> annee){
        return LocalDate
                .parse(line.split(" ")[0], FORMATTER)
                .getYear() == annee.value();
    }
    public static void logResult(Tuple2<String, Double> tuple){
        System.out.println("Ville : "+tuple._1 + " --> Prix : " + tuple._2);
    }
}

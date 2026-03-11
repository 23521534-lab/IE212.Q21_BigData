import java.io.*;
import java.util.*;

public class MovieAverageRating {
    public static void main(String[] args) throws Exception {
        Map<String, List<Double>> ratings = new HashMap<>();
        Map<String, String> movies = new HashMap<>();
        readMovies("movies.txt", movies);

        readRatings("ratings_1.txt", ratings);
        readRatings("ratings_2.txt", ratings);

        String maxMovie = "";
        double maxRating = 0.0;

        System.out.println("=== KET QUA BAI 1: DIEM TRUNG BINH CAC PHIM ===");
        System.out.println("------------------------------------------------");

        for (String movieId : ratings.keySet()) {
            List<Double> list = ratings.get(movieId);
            int count = list.size();
            double sum = 0;
            for (double r : list) sum += r;
            double avg = sum / count;

            String title = movies.getOrDefault(movieId, "Movie_" + movieId);
            System.out.printf("%s: %.2f (Total: %d ratings)%n", title, avg, count);

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = title;
            }
        }

        System.out.println("------------------------------------------------");
        if (!maxMovie.isEmpty()) {
            System.out.printf("PHIM CAO NHAT: %s voi diem %.2f (co it nhat 5 danh gia)%n", 
                    maxMovie, maxRating);
        } else {
            System.out.println("Khong tim thay phim nao co it nhat 5 danh gia");
        }
    }

    private static void readMovies(String filename, Map<String, String> movies) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",", 3);
            if (fields.length >= 2) {
                String movieId = fields[0].trim();
                String title = fields[1].trim();
                movies.put(movieId, title);
            }
        }
        br.close();
    }

    private static void readRatings(String filename, Map<String, List<Double>> ratings) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            String[] fields = line.split(", ");
            if (fields.length >= 3) {
                String movieId = fields[1].trim();
                double rating = Double.parseDouble(fields[2].trim());
                ratings.putIfAbsent(movieId, new ArrayList<>());
                ratings.get(movieId).add(rating);
            }
        }
        br.close();
    }
}

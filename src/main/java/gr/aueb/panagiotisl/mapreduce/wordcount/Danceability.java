package gr.aueb.panagiotisl.mapreduce.wordcount;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import gr.aueb.panagiotisl.mapreduce.wordcount.Danceability.SpotifyTrack;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Danceability {
    public static class DanceabilityMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header row (key = 0 for first line)
            if (key.get() == 0) {
                return;
            }

            // split a line into tokens
            String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // Line is missing information
            if (tokens.length < 25) {
                return; // skip bad lines
            }

    
            SpotifyTrack track = new SpotifyTrack(tokens);
            if (track.country == null || track.albumReleaseDate == null || track.danceability == null || track.name == null) {
                return;
            }

            // Extract month from LocalDate
           //int month = track.albumReleaseDate.getMonthValue();

            // Create a key using Country and Month
            String keyStr = track.country + "_" + track.albumReleaseDate;

            MapWritable mapValue = new MapWritable();
            mapValue.put(new Text("danceability"), new DoubleWritable(track.danceability));
            // mapValue.put(new Text("trackName"), new Text(track.name));
            context.write(new Text(keyStr), mapValue);
        }
    }

    public static class DanceabilityReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            for (MapWritable value : values) {
                DoubleWritable d = (DoubleWritable)value.get(new Text("danceability"));
                if (d != null) {
                    sum += d.get();
                    count++;
                }
            }
             if (count > 0) {
                double averageDanceability = sum / count;
                // output (country-month, average danceability)
                context.write(key, new DoubleWritable(averageDanceability));
             }
        }
    }

    public static class SpotifyTrack {
        private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("MM/dd/yyyy");

        private static String removeQuotes(String v) {
            if (v == null) return null;
            if (v.startsWith("\"") && v.endsWith("\"")) {
                return v.substring(1, v.length() - 1);
            }
            return v;
        }

        private static Double parseDouble(String v) {
            v = removeQuotes(v);
            return (v == null || v.isEmpty()) ? null : Double.valueOf(v);
        }

        private static LocalDate parseDate(String v) {
            v = removeQuotes(v);
            if (v == null || v.isEmpty()) {
                return null;
            }
            try {
                return LocalDate.parse(v, DATE_FMT);
            } catch (Exception e) {
                // Log and return null on parse failure
                System.err.println("Failed to parse date: " + v);
                return null;
            }
        }

        private static Boolean parseBoolean(String v) {
            return v != null && (v.equalsIgnoreCase("true") || v.equals("1"));
        }
        public SpotifyTrack(String[] t) {
            this.spotifyId = t[0];
            this.name = t[1];
            this.artists = t[2];

            this.dailyRank = t[3];
            this.dailyMovement = t[4];
            this.weeklyMovement = t[5];

            this.country = t[6];
            this.snapshotDate = t[7];
            this.popularity = t[8];

            this.isExplicit = t[9];

            this.durationMs = t[10];

            this.albumName = removeQuotes(t[11]);
            // this.albumReleaseDate = parseDate(t[12]);
            this.albumReleaseDate = t[12];
            this.danceability = parseDouble(t[13]);
            this.energy = t[14];
            this.key = t[15];
            this.loudness = t[16];
            this.mode = t[17];
            this.speechiness = t[18];
            this.acousticness = t[19];
            this.instrumentalness = t[20];
            this.liveness = t[21];
            this.valence = t[22];
            this.tempo = t[23];
            this.timeSignature = t[24];
        }
        public String spotifyId;
        public String name;
        public String artists;

        public String dailyRank;
        public String dailyMovement;
        public String weeklyMovement;

        public String country;
        public String snapshotDate;
        public String popularity;

        public String isExplicit;

        public String durationMs;

        public String albumName;
        // public LocalDate albumReleaseDate;
        public String albumReleaseDate;
        public Double danceability;
        public String energy;
        public String key;
        public String loudness;
        public String mode;
        public String speechiness;
        public String acousticness;
        public String instrumentalness;
        public String liveness;
        public String valence;
        public String tempo;
        public String timeSignature;

    }

   
}

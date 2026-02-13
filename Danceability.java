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
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class Danceability {
    public static class DanceabilityMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip header row (key = 0 for first line)
            context.getCounter("DEBUG", "LINES_READ").increment(1);

            
            if (key.get() == 0) {
                context.getCounter("DEBUG", "HEADER_SKIPPED").increment(1);

                return;
            }

            // split a line into tokens
            String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // Line is missing information
            if (tokens.length < 25) {
                context.getCounter("DEBUG", "BAD_LENGTH").increment(1);
                System.err.println("BAD LENGTH: " + value);
                return; // skip bad lines
            }

            SpotifyTrack track = new SpotifyTrack(tokens);
            if (track.country == null) {
                    context.getCounter("DEBUG", "NO_COUNTRY").increment(1);
                    System.err.println("NO COUNTRY: " + value);
                    return;
                }

                if (track.albumReleaseDate == null) {
                    context.getCounter("DEBUG", "NO_DATE").increment(1);
                    System.err.println("NO DATE: " + value);
                    return;
                }

                String month = SpotifyTrack.getAlbumReleaseMonth(track.albumReleaseDate);
                if (month == null) {
                    context.getCounter("DEBUG", "BAD_DATE_FORMAT").increment(1);
                    return;
                }

                if (track.danceability == null) {
                    context.getCounter("DEBUG", "NO_DANCEABILITY").increment(1);
                    System.err.println("NO DANCEABILITY: " + value);
                    return;
                }

                context.getCounter("DEBUG", "LINES_EMITTED").increment(1);

            //Line is missing information
            if (track.country == null || track.albumReleaseDate == null || track.danceability == null || track.name == null) {
                return; // skip bad lines
            }

            // Create a key using Country and Month
            String keyStr = track.country + "_"  + month; //String.format("%02d", month);

            MapWritable mapValue = new MapWritable();
            mapValue.put(new Text("danceability"), new DoubleWritable(track.danceability));
            mapValue.put(new Text("trackName"), new Text(track.name));

            context.write(new Text(keyStr), mapValue);
        }
    }

    public static class DanceabilityReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            double maxDanceability = Double.NEGATIVE_INFINITY;
            String maxTrackName = null;

            for (MapWritable value : values) {
                DoubleWritable d = (DoubleWritable)value.get(new Text("danceability"));

                if (d.get() > maxDanceability) {
                    maxDanceability = d.get();
                    maxTrackName = ((Text)value.get(new Text("trackName"))).toString();
                }
                if (d != null) {
                    sum += d.get();
                    count++;
                }
            }
             if (count > 0) {
                double averageDanceability = sum / count;
                // output (country-month, average danceability)
                context.write(key, new DoubleWritable(averageDanceability));
                // output (country-month, most danceable track)
                context.write(key, new DoubleWritable(maxDanceability));

             }
        }
    }

    public static class SpotifyTrack {
        private static String clean(String v) {
            if (v == null) return null;
            v = v.replace("\"", "").trim();
            return v.isEmpty() ? null : v;
        }

        public SpotifyTrack(String[] t) {
            this.spotifyId = t[0];
            this.name = clean(t[1]);
            this.artists = t[2];

            this.dailyRank = t[3];
            this.dailyMovement = t[4];
            this.weeklyMovement = t[5];

            this.country = clean(t[6]);
            this.snapshotDate = t[7];
            this.popularity = t[8];

            this.isExplicit = t[9];

            this.durationMs = t[10];

            this.albumName = t[11];
            this.albumReleaseDate = clean(t[12]);
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
    
        public static String getAlbumReleaseMonth(String dateStr) {
            if (dateStr == null) return null;

            String[] parts = dateStr.split("-");

            if (parts.length > 1) {
                return parts[1];   // second part
            }

            return null;
        }
  
        private static Double parseDouble(String v) {
            if (v == null || v.isEmpty()) return null;
            v = v.replace("\"", "");
            return v.isEmpty() ? null : Double.valueOf(v);
        }
    }

   
}

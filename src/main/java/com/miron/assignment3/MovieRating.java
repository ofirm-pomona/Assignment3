package com.miron.assignment3;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class MovieRating {

    static void calculate(String[] args) throws Exception {
        // MapReduce Job
        Configuration conf = new Configuration();

        Job job = new Job(conf, "movieRating");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileUtils.deleteDirectory(new File(args[2]));

        job.waitForCompletion(true);

        // Find top 10 values
        PriorityQueue<Movie> queue = new PriorityQueue<>(new Comparator<Movie>() {
            @Override
            public int compare(Movie o1, Movie o2) {
                return Float.compare(
                        o2.getRating(),
                        o1.getRating()
                );
            }
        });
        Scanner scanner = new Scanner(new File(args[2] + File.separator + "part-r-00000"));
        while (scanner.hasNext()) {
            String movieId = scanner.next();
            float rating = scanner.nextFloat();
            Movie movie = new Movie(movieId, rating);
            queue.add(movie);
        }
        for (int i = 0; i < 10; i++) {
            Movie movie = queue.poll();

            // Find movie title
            Scanner movieScanner = new Scanner(new File(args[1]));
            while (movieScanner.hasNextLine()) {
                String line = movieScanner.nextLine();
                String movieId = line.substring(0, line.indexOf(","));
                if (movie.getId().equals(movieId)) {
                    movie.setName(line.substring(line.indexOf(",")+1));
                    break;
                }
            }

            // Print
            System.out.println(movie);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text word = new Text();
        private FloatWritable FloatWritable = new FloatWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                // Get movie id
                String token = tokenizer.nextToken();
                String movieID = token.substring(0, token.indexOf(","));
                word.set(movieID);

                // Get rating
                int indexOf = token.lastIndexOf(",") + 1;
                int rating = Integer.parseInt(token.substring(indexOf, indexOf + 1));
                FloatWritable.set(rating);

                // Return result
                context.write(word, FloatWritable);
            }
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            // Calculate sum
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            // Calculate average
            sum = sum / count;

            // Return result
            context.write(key, new FloatWritable(sum));
        }
    }
}
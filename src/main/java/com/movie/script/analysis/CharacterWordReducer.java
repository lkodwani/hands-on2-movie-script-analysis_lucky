package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CharacterWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

   private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Sum up the counts for the word spoken by the character
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Set the result to the total count
        result.set(sum);

        // Write the result (character-word combination and its total count)
        context.write(key, result);
    }
}




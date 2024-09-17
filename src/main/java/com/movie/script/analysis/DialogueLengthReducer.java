package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalWords = 0;

        // Iterate through all values for a particular key (character)
        for (IntWritable val : values) {
            totalWords += val.get();
        }

        
        // Emit the character and their total word count
        context.write(key, new IntWritable(totalWords));
    }
}



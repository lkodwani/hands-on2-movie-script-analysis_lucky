package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
// Assuming each line is of the format "Character: spoken words"
        String line = value.toString();

        // Split the line into character name and their spoken words
        String[] parts = line.split(":", 2);

        // Check if the line is properly formatted
        if (parts.length == 2) {
            String characterName = parts[0].trim(); // Extract character name
            String dialogue = parts[1].trim(); // Extract spoken words

            // Tokenize the spoken words
            StringTokenizer tokenizer = new StringTokenizer(dialogue);

            // For each word, emit the combination of character and word with a count of 1
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().replaceAll("\\W+", ""); // Remove punctuation
                if (!word.isEmpty()) {
                    // Emit the combination of character and word
                    characterWord.set(characterName + " " + word.toLowerCase());
                    context.write(characterWord, one);
                }
            }
        }
        

    }
}



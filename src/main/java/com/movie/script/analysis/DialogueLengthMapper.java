package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Assume the input is in the format: "CharacterName: Dialogue"
        String line = value.toString();
        String[] parts = line.split(":", 2);

        // If the line is valid and contains both character and dialogue
        if (parts.length == 2) {
            String characterName = parts[0].trim();
            String dialogue = parts[1].trim();

            
            // Count the number of words in the dialogue
            int words = dialogue.split("\\s+").length;

            // Set the character and word count
            character.set(characterName);
            wordCount.set(words);

            // Emit the character as key and word count as value
            context.write(character, wordCount);
        }

    }
}

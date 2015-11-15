package com.ucla.storm_uda_sample.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Class for generating a random sequence of words with a certain number of uniques
 * <p>
 * Assuming UNIQUE is the number passed in CTor. The number of unique words generated varies over time:
 * The first 25 values have UNIQUE unique words
 * The next 25% values have 0.2 * UNIQUE unique words
 * The next 25% values have 0.02 * UNIQUE unique words
 * The remaining values have a 0.5 * UNIQUE unique words
 */
public class RandomStringSpout extends BaseRichSpout {
    // Collector
    private SpoutOutputCollector _collector;
    // The number of uniques to use in the sequence
    private final int uniques;
    // List of words to use
    private ArrayList<String> words;
    // Number of words remaining to generate
    private int wordsRemaining;
    // Number of words to generate in total
    private double howMany;
    // Random number generator
    private Random random;

    /**
     * CTor
     *
     * @param howMany  number of values to generate
     * @param fileName file with list of words to use
     * @param uniques  number of unique words in the sequence
     */
    public RandomStringSpout(int howMany, String fileName, int uniques) {
        this.uniques = uniques;
        this.wordsRemaining = howMany;
        this.howMany = howMany;
        this.words = new ArrayList<>();
        ArrayList<String> allWords = new ArrayList<>();
        random = ThreadLocalRandom.current();

        try {
            Scanner sc = new Scanner(new FileReader(fileName));
            while (sc.hasNextLine()) {
                allWords.add(sc.nextLine().trim());
            }

            if (uniques > allWords.size()) {
                System.out.printf("No. of uniques expected from word streamer is more than dictionary size.%nOnly %d uniques will be generated.%n", allWords.size());
                uniques = allWords.size();
            }

            shuffle(allWords);

            for (int i = 0; i < uniques; i++) {
                words.add(allWords.get(i));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Shuffle ArrayList of words
     *
     * @param array ArrayList of words
     */
    private void shuffle(ArrayList array) {
        Random random = ThreadLocalRandom.current();

        for (int i = 0; i < array.size(); i++) {
            int pos = random.nextInt(i + 1);
            Object temp = array.get(i);
            array.set(i, array.get(pos));
            array.set(pos, temp);
        }
    }

    /**
     * Override of the declareOutputFields method
     *
     * @param declarer declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    /**
     * Override of the open method
     *
     * @param conf      conf map
     * @param context   context
     * @param collector collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    /**
     * Override of the nextTuple method
     */
    @Override
    public void nextTuple() {
        if (wordsRemaining-- > 0) {
            int u;
            if (wordsRemaining > (0.75 * (double) howMany)) {
                u = (int) ((double) uniques * 1.0);
            } else if (wordsRemaining > (0.5 * (double) howMany)) {
                u = (int) ((double) uniques * 0.2);
            } else if (wordsRemaining > (0.25 * (double) howMany)) {
                u = (int) ((double) uniques * 0.02);
            } else {
                u = (int) ((double) uniques * 0.5);
            }

            int pos = random.nextInt(u);
            String s = words.get(pos);
            _collector.emit(new Values(s));

            if (wordsRemaining % 1000 == 0) {
                System.out.println(wordsRemaining + " remaining");
            }
        }
    }
}

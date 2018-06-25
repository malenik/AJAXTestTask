package com.malenko.ajaxtask;

import akka.stream.Materializer;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;


public class CsvProcessor {

    private final Materializer mat;
    private final Path source;

    public CsvProcessor(Path source, Materializer mat) {
        this.mat = mat;
        this.source = source;
    }

    public CompletionStage<Statistic> split(Path destination) {
        return FileIO.fromPath(source)
                .via(Framing.delimiter(ByteString.fromString("\n"), 1024))  //splits files by lines
                .map(ByteString::utf8String)                    // makes a String from ByteString
                .scan(Statistic.empty, (acc, line) -> {         // returns current line and dynamically makes statistic (every element returns new instance of Statistic that depends on previous Statistic)
                    HashMap<String, Integer> statistic = acc.getStatistic();
                    String key = line.split(",")[0];
                    statistic.put(key, statistic.getOrDefault(key, 0) + 1);
                    return Statistic.apply(statistic, line);
                })
                .filter(it -> it.getLine() != null)             // rid of first empty line
                .alsoTo(Sink.foreach(statistic -> {             // sink that writes elements to file
                    String line = statistic.getLine();
                    String filename = line.split(",")[0] + ".csv";
                    Path path = destination.resolve(filename);
                    Files.write(
                            path,
                            (line + "\n").getBytes(),
                            Files.exists(path) ? APPEND : CREATE
                    );
                }))
                .toMat(Sink.last(), Keep.right())               // sink that returns last Statistic instance
                .run(mat);
    }
}

class Statistic {

    private String line;
    private HashMap<String, Integer> statistic;

    public Statistic(HashMap<String, Integer> statistic, String line) {
        this.statistic = statistic;
        this.line = line;
    }

    public static Statistic empty = new Statistic(new HashMap<>(), null);

    public static Statistic apply(HashMap<String, Integer> statistic, String line) {
        return new Statistic(statistic, line);
    }

    public HashMap<String, Integer> getStatistic() {
        return statistic;
    }

    public String getLine() {
        return line;
    }

    public void prettyPrint() {
        Integer total = statistic.values().stream().mapToInt(num -> num).sum();
        System.out.println("Total records was processed: " + total);
        statistic.entrySet().stream()
                .filter(entry -> !entry.getKey().equals("total"))
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .forEach(entry -> {
                    String name = entry.getKey();
                    Integer num = entry.getValue();
                    System.out.println(String.format("Records in '%s.csv' was processed: %d (%d%%)", name, num, 100 * num / total));
                });
    }
}
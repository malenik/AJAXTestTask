package com.malenko.ajaxtask;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class Launcher {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ActorSystem actorSystem = ActorSystem.create("csv-processor");
        Materializer mat = ActorMaterializer.create(actorSystem);
        Path source = Paths.get("./src/main/resources/jun-mid-task.csv");
        Path destination = Paths.get("./src/main/resources");

        CsvProcessor csvProcessor = new CsvProcessor(source, mat);
        CompletionStage<Statistic> futureResult = csvProcessor.split(destination);

        Statistic statistic = futureResult.toCompletableFuture().get();
        statistic.prettyPrint();
    }
}

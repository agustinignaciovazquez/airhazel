package ar.edu.itba.pod.api.query;

import ar.edu.itba.pod.api.collator.CalculatePercentageAirlineCollator;
import ar.edu.itba.pod.api.combiner.SumCombinerFactory;
import ar.edu.itba.pod.api.mapper.FlightPerAirlineMapper;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.FlightClassification;
import ar.edu.itba.pod.api.model.enums.field.FlightField;
import ar.edu.itba.pod.api.predicates.KeyStringPredicate;
import ar.edu.itba.pod.api.reducer.CountReducerFactory;
import ar.edu.itba.pod.api.util.FileReader;
import ar.edu.itba.pod.api.util.FlightImporter;
import ar.edu.itba.pod.api.util.ParallelFileReader;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class CabotageFlightsPerAirlineQuery extends Query{
    private MultiMap<String, Flight> flightsMultiMap;
        private int n;
        private List<Map.Entry<String, Double>> result;

        private static Logger LOGGER = LoggerFactory.getLogger(FlightsPerOriginAirportQuery.class);

        public CabotageFlightsPerAirlineQuery(HazelcastInstance hazelcastInstance, File airportsFile,
                                              File flightsFile, int n) {
            super(hazelcastInstance, airportsFile, flightsFile);
            this.n = n;
        }

        @Override
        public void readFiles() {
            FileReader fileReader = new ParallelFileReader();

            Collection<Flight> flights = null;
            try {
                flights = fileReader.readFlights(getFlightsFile());
            } catch (IOException e) {
                LOGGER.error("I/O Exception while reading input files");
                System.exit(1);
            }

            flightsMultiMap = getHazelcastInstance().getMultiMap("flights");

            FlightImporter flightImporter = new FlightImporter();
            flightImporter.importToMultiMap(flightsMultiMap, flights, FlightField.FLIGHT_CLASSIFICATION);
        }

    @Override
    public void mapReduce() {
        JobTracker jobTracker = getHazelcastInstance().getJobTracker("airline-flight-airport-count");
        final KeyValueSource<String, Flight> source = KeyValueSource.fromMultiMap(flightsMultiMap);
        Job<String, Flight> job = jobTracker.newJob(source);
        //List<String> keys = Arrays.asList(FlightClassification.CABOTAGE.getName(),FlightClassification.NOT_AVAILABLE.getName());
        ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                //.keyPredicate( new KeyStringListPredicate(keys))
                .keyPredicate( new KeyStringPredicate(FlightClassification.CABOTAGE.getName()))
                .mapper(new FlightPerAirlineMapper())
                .combiner(new SumCombinerFactory<>())
                .reducer(new CountReducerFactory<>())
                .submit(new CalculatePercentageAirlineCollator(n));


        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("Hazelcast Exception");
        }
    }

    @Override
    public void log(Path path) {
        String header = "Aerolinea;Porcentaje\n";
        try {
            Files.write(path, header.getBytes());
            DecimalFormat dc = new DecimalFormat("#.##");
            Map.Entry<String, Double> others = null;
            for (Map.Entry<String, Double> e : result) {

                String airline = e.getKey();
                String out = airline + ";" + dc.format(e.getValue()) + "%\n";
                Files.write(path, out.getBytes(), StandardOpenOption.APPEND);
            }
            //flightsMultiMap.clear();

        } catch (IOException e) {
            LOGGER.error("Error writing to out file");
        }
    }
}

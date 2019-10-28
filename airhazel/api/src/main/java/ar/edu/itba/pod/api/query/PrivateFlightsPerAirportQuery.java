package ar.edu.itba.pod.api.query;

import ar.edu.itba.pod.api.collator.PercentageFlightCollator;
import ar.edu.itba.pod.api.combiner.FlightPredicateCombinerFactory;
import ar.edu.itba.pod.api.mapper.FlightPredicatePerAirportMapper;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.FlightClass;
import ar.edu.itba.pod.api.reducer.PercentageFlightReducerFactory;
import ar.edu.itba.pod.api.util.FileReader;
import ar.edu.itba.pod.api.util.FlightImporter;
import ar.edu.itba.pod.api.util.ParallelFileReader;
import com.hazelcast.core.*;
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
import java.util.*;
import java.util.concurrent.ExecutionException;

public class PrivateFlightsPerAirportQuery extends Query {
    private IList<Flight> flightsIList;
    private int n;
    private Set<Map.Entry<String, Double>> result;

    private static Logger LOGGER = LoggerFactory.getLogger(PrivateFlightsPerAirportQuery.class);

    public PrivateFlightsPerAirportQuery(HazelcastInstance hazelcastInstance, File airportsFile,
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

        flightsIList = getHazelcastInstance().getList("flights");

        FlightImporter flightImporter = new FlightImporter();
        flightImporter.importToIList(flightsIList, flights);
    }

    public void mapReduce(){
        JobTracker jobTracker = getHazelcastInstance().getJobTracker("private-flight-per-airport-count");
        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(flightsIList);
        Job<String, Flight> job = jobTracker.newJob(source);

        ICompletableFuture<Set<Map.Entry<String, Double>>> future = job
                .mapper( new FlightPredicatePerAirportMapper((flight -> {
                    List<FlightClass> flightClasses = Arrays.asList(FlightClass.PRIVATE_INTERNATIONAL,FlightClass.PRIVATE_NATIONAL);
                    return flightClasses.stream().anyMatch(fc -> fc == flight.getFlightClass());
                })))
                .combiner( new FlightPredicateCombinerFactory<>())
                .reducer(new PercentageFlightReducerFactory())
                .submit(new PercentageFlightCollator(n));
        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("Hazelcast Exception");
        }
    }

    @Override
    public void log(Path path) {
        String header = "OACI;Porcentaje\n";
        try {
            Files.write(path, header.getBytes());
            for (Map.Entry<String, Double> e : result) {
                String oaci = e.getKey();
                String out = oaci + ";" + e.getValue() + "\n";
                Files.write(path, out.getBytes(), StandardOpenOption.APPEND);
            }
        }
        catch (IOException e) {
            LOGGER.error("Error writing to out file");
        }
    }
}

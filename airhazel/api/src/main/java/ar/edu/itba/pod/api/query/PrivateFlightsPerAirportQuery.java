package ar.edu.itba.pod.api.query;

import ar.edu.itba.pod.api.collator.FlightsPerAirportCollator;
import ar.edu.itba.pod.api.collator.PrivateFlightPerAirportCollator;
import ar.edu.itba.pod.api.combiner.PrivateFlightCombinerFactory;
import ar.edu.itba.pod.api.combiner.SumCombinerFactory;
import ar.edu.itba.pod.api.mapper.FlightPerAirportMapper;
import ar.edu.itba.pod.api.mapper.FlightTypePerAirportMapper;
import ar.edu.itba.pod.api.mapper.PrivateFlightPerAirportMapper;
import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.FlightClass;
import ar.edu.itba.pod.api.model.enums.FlightType;
import ar.edu.itba.pod.api.model.enums.field.AirportField;
import ar.edu.itba.pod.api.model.enums.field.FlightField;
import ar.edu.itba.pod.api.predicates.KeyStringListPredicate;
import ar.edu.itba.pod.api.predicates.KeyStringPredicate;
import ar.edu.itba.pod.api.reducer.CountReducerFactory;
import ar.edu.itba.pod.api.reducer.PrivateFlightReducerFactory;
import ar.edu.itba.pod.api.util.AirportImporter;
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
    private MultiMap<String, Flight> flightsMultiMap;
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

        flightsMultiMap = getHazelcastInstance().getMultiMap("flights");

        FlightImporter flightImporter = new FlightImporter();
        flightImporter.importToMultiMap(flightsMultiMap, flights, FlightField.FLIGHT_CLASS);
    }

    public void mapReduce(){
        JobTracker jobTracker = getHazelcastInstance().getJobTracker("private-flight-per-airport-count");
        final KeyValueSource<String, Flight> source = KeyValueSource.fromMultiMap(flightsMultiMap);
        Job<String, Flight> job = jobTracker.newJob(source);

        List<String> StringList = Arrays.asList(FlightClass.PRIVATE_NATIONAL.getName(),FlightClass.PRIVATE_INTERNATIONAL.getName());
        ICompletableFuture<Set<Map.Entry<String, Double>>> future = job
                .mapper( new PrivateFlightPerAirportMapper())
                .combiner( new PrivateFlightCombinerFactory<>())
                .reducer(new PrivateFlightReducerFactory())
                .submit(new PrivateFlightPerAirportCollator(n));
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

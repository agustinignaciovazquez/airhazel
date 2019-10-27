package ar.edu.itba.pod.api.query;

import ar.edu.itba.pod.api.collator.FlightsPerAirportCollator;
import ar.edu.itba.pod.api.combiner.SumCombinerFactory;
import ar.edu.itba.pod.api.mapper.FlightTypePerAirportMapper;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.FlightType;
import ar.edu.itba.pod.api.model.enums.field.FlightField;
import ar.edu.itba.pod.api.predicates.KeyStringPredicate;
import ar.edu.itba.pod.api.reducer.CountReducerFactory;
import ar.edu.itba.pod.api.util.FileReader;
import ar.edu.itba.pod.api.util.FlightImporter;
import ar.edu.itba.pod.api.util.ParallelFileReader;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MultiMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlightsPerOriginAirportQuery extends Query {
    private MultiMap<String, Flight> flightsMultiMap;
    private String originOaci;
    private int n;
    private Set<Map.Entry<String, Long>> result;

    private static Logger LOGGER = LoggerFactory.getLogger(FlightsPerOriginAirportQuery.class);

    public FlightsPerOriginAirportQuery(HazelcastInstance hazelcastInstance, File airportsFile,
                                        File flightsFile, String originOaci, int n) {
        super(hazelcastInstance, airportsFile, flightsFile);
        this.originOaci = originOaci;
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
        flightImporter.importToMultiMap(flightsMultiMap, flights, FlightField.ORIGIN_OACI);
    }

    @Override
    public void mapReduce() {
        JobTracker jobTracker = getHazelcastInstance().getJobTracker("flight-per-origin-airport-count");
        final KeyValueSource<String, Flight> source = KeyValueSource.fromMultiMap(flightsMultiMap);
        Job<String, Flight> job = jobTracker.newJob(source);

        ICompletableFuture<Set<Map.Entry<String, Long>>> future = job
                .keyPredicate( new KeyStringPredicate(originOaci))
                .mapper( new FlightTypePerAirportMapper(FlightType.DEPARTURE, FlightField.DESTINATION_OACI))
                .combiner( new SumCombinerFactory<>() )
                .reducer( new CountReducerFactory<>() )
                .submit( new FlightsPerAirportCollator() );

        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("Hazelcast Exception");
        }
    }

    @Override
    public void log(Path path) {
        String header = "OACI;Despegues\n";
        try {
            Files.write(path, header.getBytes());
            int count = 0;
            for (Map.Entry<String, Long> e : result) {
                String oaci = e.getKey();
                String out = oaci + ";" + e.getValue() + "\n";
                Files.write(path, out.getBytes(), StandardOpenOption.APPEND);
                count++;
                if (count == n)
                    break;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("I/O Exception while writing output logs");
        }
    }
}


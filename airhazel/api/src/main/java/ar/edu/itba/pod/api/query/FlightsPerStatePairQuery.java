package ar.edu.itba.pod.api.query;

import ar.edu.itba.pod.api.collator.FlightsPerPairMinimumCollator;
import ar.edu.itba.pod.api.combiner.SumCombinerFactory;
import ar.edu.itba.pod.api.mapper.FlightPerStatePairMapper;
import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.Pair;
import ar.edu.itba.pod.api.model.enums.field.AirportField;
import ar.edu.itba.pod.api.reducer.CountReducerFactory;
import ar.edu.itba.pod.api.util.AirportImporter;
import ar.edu.itba.pod.api.util.FileReader;
import ar.edu.itba.pod.api.util.FlightImporter;
import ar.edu.itba.pod.api.util.ParallelFileReader;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class FlightsPerStatePairQuery extends Query{
    private IList<Flight> flightsIList;
    private IMap<String, Airport> airportIMap;
    private Set<Map.Entry<Pair<String, String>, Long>> result;
    private final Long min;
    private final String randomString;
    private static Logger LOGGER = LoggerFactory.getLogger(FlightsPerStatePairQuery.class);

    public FlightsPerStatePairQuery(HazelcastInstance hazelcastInstance, File airportsFile, File flightsFile, Long min) {
        super(hazelcastInstance, airportsFile, flightsFile);
        this.min = min;
        this.randomString = RandomStringUtils.random(10, true, true);
    }

    public void readFiles(){

        FileReader fileReader = new ParallelFileReader();

        Collection<Airport> airports = null;
        Collection<Flight> flights = null;
        try {
            airports = fileReader.readAirports(getAirportsFile());
            flights = fileReader.readFlights(getFlightsFile());
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("I/O Exception while reading input files");
            System.exit(1);
        }

        flightsIList = getHazelcastInstance().getList("g13-flights-"+randomString);
        airportIMap = getHazelcastInstance().getMap("g13-airports-"+randomString);

        FlightImporter flightsImporter = new FlightImporter();
        AirportImporter airportImporter = new AirportImporter();

        flightsImporter.importToIList(flightsIList, flights);
        airportImporter.importToIMap(airportIMap, airports, AirportField.OACI);
    }

    public void mapReduce(){

        JobTracker jobTracker = getHazelcastInstance().getJobTracker("flight-per-state-count");
        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(flightsIList);
        Job<String, Flight> job = jobTracker.newJob(source);

        JobCompletableFuture<Set<Map.Entry<Pair<String, String>, Long>>> future = job
                .mapper( new FlightPerStatePairMapper(randomString) )
                .combiner( new SumCombinerFactory<>() )
                .reducer( new CountReducerFactory<>() )
                .submit( new FlightsPerPairMinimumCollator(this.min) );

        result = null;
        try {
            result = future.get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("Hazelcast Exception");
        }
    }

    @Override
    public void log(Path path) {

        String header = "Provincia A;Provincia B;Movimientos\n";
        try{
            Files.write(path, header.getBytes());
            for (Map.Entry<Pair<String, String>, Long> e : result){
                String out = e.getKey().getKey()+";"+e.getKey().getValue()+";"+e.getValue()+"\n";
                Files.write(path, out.getBytes(), StandardOpenOption.APPEND);
            }
            /* Clear hazelcast collections */
            airportIMap.clear();
            flightsIList.clear();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("I/O Exception while writing output logs");
        }
    }
}
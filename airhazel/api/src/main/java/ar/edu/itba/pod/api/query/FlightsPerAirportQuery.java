package ar.edu.itba.pod.api.query;

import ar.edu.itba.pod.api.collator.FlightsCollator;
import ar.edu.itba.pod.api.combiner.SumCombinerFactory;
import ar.edu.itba.pod.api.mapper.FlightPerAirportMapper;
import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.field.AirportField;
import ar.edu.itba.pod.api.reducer.CountReducerFactory;
import ar.edu.itba.pod.api.util.AirportImporter;
import ar.edu.itba.pod.api.util.FileReader;
import ar.edu.itba.pod.api.util.FlightImporter;
import ar.edu.itba.pod.api.util.ParallelFileReader;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class FlightsPerAirportQuery extends Query {
    private IList<Flight> flightsIList;
    private IMap<String, Airport> airportIMap;
    private List<Map.Entry<String, Long>> result;
    private final String randomString;
    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FlightsPerAirportQuery.class);

    public FlightsPerAirportQuery(HazelcastInstance hazelcastInstance, File airportsFile, File flightsFile) {
        super(hazelcastInstance, airportsFile, flightsFile);
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

        JobTracker jobTracker = getHazelcastInstance().getJobTracker("flight-count");
        final KeyValueSource<String, Flight> source = KeyValueSource.fromList(flightsIList);
        Job<String, Flight> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper( new FlightPerAirportMapper(randomString) )
                .combiner( new SumCombinerFactory<>() )
                .reducer( new CountReducerFactory<>() )
                .submit( new FlightsCollator() );

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

        String header = "OACI;Denominación;Movimientos\n";
        try {
            Files.write(path, header.getBytes());

            for (Map.Entry<String, Long> e : result) {
                String oaci = e.getKey();
                String out = oaci + ";" + airportIMap.get(oaci).getAirportName() + ";" + e.getValue() + "\n";
                Files.write(path, out.getBytes(), StandardOpenOption.APPEND);
            }
            /* Clear hazelcast collections */
            flightsIList.clear();
            airportIMap.clear();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("I/O Exception while writing output logs");
        }
    }
}

package ar.edu.itba.pod.api.query;

import com.hazelcast.core.HazelcastInstance;

import java.io.File;
import java.nio.file.Path;

public abstract class Query{

    private HazelcastInstance hazelcastInstance;
    private File airportsFile;
    private File flightsFile;

    public Query(HazelcastInstance hazelcastInstance, File airportsFile, File flightsFile) {
        this.hazelcastInstance = hazelcastInstance;
        this.airportsFile = airportsFile;
        this.flightsFile = flightsFile;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public File getAirportsFile() {
        return airportsFile;
    }

    public File getFlightsFile() {
        return flightsFile;
    }

    public abstract void readFiles();

    public abstract void mapReduce();

    public abstract void log(Path path);
}

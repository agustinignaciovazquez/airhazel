package ar.edu.itba.pod.api.query;

import com.hazelcast.core.HazelcastInstance;

import java.io.File;
import java.nio.file.Path;

public abstract class Query{

    private HazelcastInstance client;
    private File airportsFile;
    private File movementsFile;

    public Query(HazelcastInstance client, File airportsFile, File movementsFile) {
        this.client = client;
        this.airportsFile = airportsFile;
        this.movementsFile = movementsFile;
    }

    public HazelcastInstance getClient() {
        return client;
    }

    public File getAirportsFile() {
        return airportsFile;
    }

    public File getMovementsFile() {
        return movementsFile;
    }

    public abstract void readFiles();

    public abstract void mapReduce();

    public abstract void log(Path path);
}

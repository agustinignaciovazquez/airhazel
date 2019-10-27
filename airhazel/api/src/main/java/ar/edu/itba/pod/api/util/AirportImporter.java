package ar.edu.itba.pod.api.util;
import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.enums.field.Field;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.util.*;

public class AirportImporter implements Importer<Airport> {

    public AirportImporter() {
    }

    @Override
    public void importToIList(IList<Airport> iList, Collection<Airport> collection) {
        iList.addAll(collection);
    }

    @Override
    public void importToIMap(IMap<String, Airport> iMap, Collection<Airport> collection, Field field) {
        collection.parallelStream().forEach(airport -> iMap.put(airport.getField(field), airport));
    }

    @Override
    public void importToMultiMap(MultiMap<String, Airport> multiMap, Collection<Airport> collection, Field field) {
        collection.parallelStream().forEach(airport -> multiMap.put(airport.getField(field), airport));
    }
}
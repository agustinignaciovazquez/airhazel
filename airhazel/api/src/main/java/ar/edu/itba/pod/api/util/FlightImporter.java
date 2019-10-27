package ar.edu.itba.pod.api.util;

import ar.edu.itba.pod.api.model.Flight;
import ar.edu.itba.pod.api.model.enums.field.Field;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.util.Collection;

public class FlightImporter implements Importer<Flight> {
    @Override
    public void importToIList(IList<Flight> iList, Collection<Flight> collection) {
        iList.addAll(collection);
    }

    @Override
    public void importToIMap(IMap<String, Flight> iMap, Collection<Flight> collection, Field field) {
        collection.parallelStream().forEach(flight -> iMap.put(flight.getField(field), flight));
    }

    @Override
    public void importToMultiMap(MultiMap<String, Flight> multiMap, Collection<Flight> collection, Field field) {
        collection.parallelStream().forEach(flight -> multiMap.put(flight.getField(field), flight));
    }
}

package ar.edu.itba.pod.api.util;

import ar.edu.itba.pod.api.model.Airport;
import ar.edu.itba.pod.api.model.Flight;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public interface FileReader {

    Collection<Airport> readAirports(File airportsFile) throws IOException;

    Collection<Flight> readFlights(File flightFile) throws IOException;

}
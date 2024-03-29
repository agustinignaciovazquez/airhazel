package ar.edu.itba.pod.api.model;

import ar.edu.itba.pod.api.model.enums.*;
import ar.edu.itba.pod.api.model.enums.field.Field;
import ar.edu.itba.pod.api.model.enums.field.FlightField;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class Flight implements DataSerializable {

    private static final AtomicInteger count = new AtomicInteger(0);
    private Integer id = null;
    private FlightClassification flightClassification;
    private FlightClass flightClass;
    private FlightType flightType;
    private String originOaci;
    private String destinationOaci;
    private String airlineName;

    public Flight(){}

    public Flight(FlightClassification flightClassification, FlightClass flightClass,
                  FlightType flightType, String originOaci, String destinationOaci, String airlineName) {
        if(flightClass == null || flightClassification == null || flightType == null){
            throw new IllegalArgumentException();
        }
        this.id = count.incrementAndGet();
        this.flightClassification = flightClassification;
        this.flightClass = flightClass;
        this.flightType = flightType;
        this.originOaci = Optional.ofNullable(originOaci).orElse("N/A");
        this.destinationOaci = Optional.ofNullable(destinationOaci).orElse("N/A");
        this.airlineName = Optional.ofNullable(airlineName).orElse("");

    }

    public Flight(String flightClassification, String flightClass,
                  String flightType, String originOaci, String destinationOaci,String airlineName) {

        this(FlightClassification.fromString(flightClassification),
                FlightClass.fromString(flightClass),
                FlightType.fromString(flightType),
                originOaci,destinationOaci,airlineName);

    }

    public Integer getId() {
        return id;
    }

    public FlightClassification getFlightClassification() {
        return flightClassification;
    }

    public FlightClass getFlightClass() {
        return flightClass;
    }

    public FlightType getFlightType() {
        return flightType;
    }

    public String getOriginOaci() {
        return originOaci;
    }

    public String getDestinationOaci() {
        return destinationOaci;
    }

    public String getAirlineName() {
        return airlineName;
    }

    public String getField(Field field) {
        if(!(field instanceof FlightField))
            throw new IllegalArgumentException();

        FlightField flightField = (FlightField) field;
        switch (flightField) {
            case FLIGHT_CLASS:
                return this.getFlightClass().getName();
            case FLIGHT_CLASSIFICATION:
                return this.getFlightClassification().getName();
            case FLIGHT_TYPE:
                return this.getFlightType().getName();
            case ORIGIN_OACI:
                return this.getOriginOaci();
            case DESTINATION_OACI:
                return this.getDestinationOaci();
            case AIRLINE_NAME:
                return this.getAirlineName();
        }

        throw new IllegalArgumentException();
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeInt(this.id);
        objectDataOutput.writeUTF(this.flightClassification.getName());
        objectDataOutput.writeUTF(this.flightClass.getName());
        objectDataOutput.writeUTF(this.flightType.getName());
        objectDataOutput.writeUTF(this.originOaci);
        objectDataOutput.writeUTF(this.destinationOaci);
        objectDataOutput.writeUTF(this.airlineName);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.id = objectDataInput.readInt();
        this.flightClassification = FlightClassification.fromString(objectDataInput.readUTF());
        this.flightClass = FlightClass.fromString(objectDataInput.readUTF());
        this.flightType = FlightType.fromString(objectDataInput.readUTF());
        this.originOaci = objectDataInput.readUTF();
        this.destinationOaci = objectDataInput.readUTF();
        this.airlineName = objectDataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Flight flight = (Flight) o;
        return id.equals(flight.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}

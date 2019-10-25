package ar.edu.itba.pod.api.model;

import ar.edu.itba.pod.api.model.enums.FlightClass;
import ar.edu.itba.pod.api.model.enums.FlightClassification;
import ar.edu.itba.pod.api.model.enums.FlightType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class Flight implements DataSerializable {

    private static final AtomicInteger count = new AtomicInteger(0);
    private Integer id = null;
    private FlightClassification flightClassification;
    private FlightClass flightClass;
    private FlightType flightType;
    private String originOaci;
    private String destinationOaci;

    public Flight(){}

    public Flight(FlightClassification flightClassification, FlightClass flightClass,
                  FlightType flightType, String originOaci, String destinationOaci) {
        this.id = count.incrementAndGet();
        this.flightClassification = flightClassification;
        this.flightClass = flightClass;
        this.flightType = flightType;
        this.originOaci = originOaci;
        this.destinationOaci = destinationOaci;
    }

    public Flight(String flightClassification, String flightClass,
                  String flightType, String originOaci, String destinationOaci) {
        this.id = count.incrementAndGet();
        this.flightClassification = FlightClassification.fromString(flightClassification);
        this.flightClass = FlightClass.fromString(flightClass);
        this.flightType = FlightType.fromString(flightType);
        this.originOaci = originOaci;
        this.destinationOaci = destinationOaci;
    }

    //TODO VER ALGUNA FORMA DE GUARDAR EL ENUM Y NO EL STRING
    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeInt(this.id);
        objectDataOutput.writeUTF(this.flightClassification.getName());
        objectDataOutput.writeUTF(this.flightClass.getName());
        objectDataOutput.writeUTF(this.flightType.getName());
        objectDataOutput.writeUTF(this.originOaci);
        objectDataOutput.writeUTF(this.destinationOaci);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        this.id = objectDataInput.readInt();
        this.flightClassification = FlightClassification.fromString(objectDataInput.readUTF());
        this.flightClass = FlightClass.fromString(objectDataInput.readUTF());
        this.flightType = FlightType.fromString(objectDataInput.readUTF());
        this.originOaci = objectDataInput.readUTF();
        this.destinationOaci = objectDataInput.readUTF();
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

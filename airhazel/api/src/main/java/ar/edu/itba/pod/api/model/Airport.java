package ar.edu.itba.pod.api.model;
import ar.edu.itba.pod.api.model.enums.field.AirportField;
import ar.edu.itba.pod.api.model.enums.field.Field;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Airport implements DataSerializable{
    private String oaci;
    private String airportName;
    private String state;

    public Airport(){}

    public Airport(String oaci, String airportName, String state) {
        this.oaci = oaci;
        this.airportName = airportName;
        this.state = state;
    }

    public String getOaci() {
        return oaci;
    }

    public String getAirportName() {
        return airportName;
    }

    public String getState() {
        return state;
    }

    public String getField(Field field) {
        if(!(field instanceof AirportField))
            throw new IllegalArgumentException();

        AirportField airportField = (AirportField) field;
        switch (airportField) {
            case OACI:
                return this.getOaci();
            case AIRPORT_NAME:
                return this.getAirportName();
            case STATE:
                return this.getState();
        }

        throw new IllegalArgumentException();
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(oaci);
        objectDataOutput.writeUTF(airportName);
        objectDataOutput.writeUTF(state);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        oaci = objectDataInput.readUTF();
        airportName = objectDataInput.readUTF();
        state = objectDataInput.readUTF();
    }
}

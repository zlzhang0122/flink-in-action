package com.github.flink.cep;

/**
 * @Author: zlzhang0122
 * @Date: 2020/12/30 4:45 下午
 */
public class TemperatureEvent {
    private int id;
    private String machineName;
    private double temperature;

    public TemperatureEvent(int id, String machineName, double temperature) {
        this.id = id;
        this.machineName = machineName;
        this.temperature = temperature;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMachineName() {
        return machineName;
    }

    public void setMachineName(String machineName) {
        this.machineName = machineName;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "TemperatureEvent{" +
                "id=" + id +
                ", machineName='" + machineName + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}

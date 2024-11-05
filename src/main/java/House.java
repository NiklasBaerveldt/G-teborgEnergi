import java.util.Random;

public class House {
    private final double monthlyEnergyConsumption;
    private final double hourlyEnergyConsumption;
    Random random = new Random();

    public House(){
        //monthlyEnergyConsumption is randomly generated (values are between the highest and lowest for households for the states in USA)
        monthlyEnergyConsumption = 594 + (1192 - 594)* random.nextFloat(); //kWh/month
        hourlyEnergyConsumption = monthlyEnergyConsumption/720; // 720 hours in a month
    }
    public House(double monthlyEnergyConsumption){
        this.monthlyEnergyConsumption = monthlyEnergyConsumption;
        hourlyEnergyConsumption = this.monthlyEnergyConsumption/720;
    }

    //Hourly reading returns a value based on hourlyEnergyConsumption.
    //The value returned is on average highest 7-8 and 18 to 24. It is lowest during the night and slightly higher during the day.
    public double getHourlyReading(int hour){
        double energyConsumption;
        double variation = 1.0 + random.nextFloat();

        if (hour >= 0 && hour <= 7) {
            // Nighttime:
            energyConsumption = hourlyEnergyConsumption * 0.5 * variation; //returns a value between hourlyEnergyConsumption *0.5 and *1
        } else if (hour >= 8 && hour <= 18) {
            // Daytime:
            energyConsumption = hourlyEnergyConsumption * variation; //returns a value between hourlyEnergyConsumption *1 and *2
        } else {
            // Evening and morning:
            energyConsumption = hourlyEnergyConsumption * 1.5 * variation; //returns a value between hourlyEnergyConsumption *1.5 and *3
        }
        return energyConsumption;

    }
}


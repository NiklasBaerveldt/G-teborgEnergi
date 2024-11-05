import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class DataGenerator {
    private int number_of_households;
    private int hours;
    private String filename;

    public DataGenerator(int number_of_households, int duration_in_days, String filename){
        this.number_of_households = number_of_households;
        this.hours = 24*duration_in_days;
        this.filename = filename;

    }
    //Generates timestamped energy data for each house for several hours and puts it in csv file.

    public void generateData() {
        ArrayList<House> houses = new ArrayList<House>();
        for(int i = 0; i <= number_of_households; i++){
            houses.add(new House());
        }
        try (FileWriter writer = new FileWriter(filename)) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            for (int h = 0; h < hours; h++) {
                LocalDateTime timestamp = LocalDateTime.now().minusHours(hours - h);
                for (int household = 0; household < number_of_households; household++) {
                    int hour = timestamp.getHour();
                    double reading = houses.get(household).getHourlyReading(hour);
                    writer.write(String.format("%s,%d,%.2f\n", timestamp.format(formatter), household, reading));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
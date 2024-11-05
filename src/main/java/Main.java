
public class Main {

    public static void main(String[] args) {
        DataGenerator generator = new DataGenerator(10,30,"household_readings.csv");
        generator.generateData();
        String[] strings = {};
        try{
            FindGrowingAverageQuery.main(strings);
        }catch (Exception e)  {
            throw new RuntimeException(e);
        } ;
    }
}
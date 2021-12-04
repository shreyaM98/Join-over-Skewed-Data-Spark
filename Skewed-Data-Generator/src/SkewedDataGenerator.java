//Note- It can't create files greater than 150 MB
// - gives Exception in thread "main" java.lang.OutOfMemoryError:
// Java heap space (might be because of too many lines written in buffer)
//importing java libraries
import java.util.Random;
import java.util.List;
import java.util.Scanner;
import java.lang.StringBuilder;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;

//class CreateDatasets
public class SkewedDataGenerator {
    // to generate random numbers
    Random rad = new Random();
    private RandomFieldGenerator randomFieldGenerator;


    // method to create records for Skewed Dataset TransactionSkewed
    public void createSkewedData(int numRecords, float skew, int numSkewKey, String filepath) throws IOException {
        //transaction: transID,custID,TransTotal,transNumItems,transDesc
        int custID, transNumItems, transID, transTotal;
        String transDesc;
        randomFieldGenerator = new RandomFieldGenerator();
        // define range of lengths for each field
        int min_items = 10, max_items = 70, min_desc = 10, max_desc = 20;
        BufferedWriter writer = new BufferedWriter(new FileWriter(filepath));
        // generating rows with skewed keys
        // suppose skew = 0.6 then 60% of total rows belong to skewed key
        int lastTransId = 0;
        for (int i = 0; i < skew * numRecords; i++) {
            // generating random customer id/key
            // if numSkewKey=1 then skewed key id is 1, if numSkewKey=2 then skewed key ids
            // are 1,2 and so on..
            transID = i;
            lastTransId=transID;

            custID = rad.nextInt(numSkewKey) + 1;
            // generating random transNumItems of length 1-10
            //transNumItems = rad.nextInt(max_items - min_items + 1) + min_items;
            transNumItems = randomFieldGenerator.getRandomInteger(1,10);
            // generating random string transDesc of length 10-20
            //transDesc = generateRandomString(rad.nextInt(max_desc - min_desc + 1) + min_desc);
            transDesc = randomFieldGenerator.getRandomString(20,50);
            transTotal = randomFieldGenerator.getRandomInteger(10,1000);
//			rating = rad.nextBoolean() ? "good" : "bad";
            // adding record into list
            writer.write(custID + "," +transID+ "," + transNumItems + "," + transDesc + ","+ transTotal +"\n");
        }
        lastTransId++;

        //generating random keys for remaining rows (non-skewed)
        //suppose skew = 0.6 then 40% of total rows belong to nonskewed keys
        for (int i = 0; i < (numRecords - skew * numRecords); i++) {
            // generating random keys from range numSkewKey to numRecords
            // excludes skew key ids
            custID = rad.nextInt(numRecords - numSkewKey + 1) + numSkewKey;
            transID = lastTransId + i;
            // generating random TransNumItems of length 1-10
            //transNumItems = rad.nextInt(max_items - min_items + 1) + min_items;
            transNumItems = randomFieldGenerator.getRandomInteger(1,10);
            // generating random string TransDesc of length 10-20
            //transDesc = generateRandomString(rad.nextInt(max_desc - min_desc + 1) + min_desc);
            transDesc = randomFieldGenerator.getRandomString(20,50);
//			rating = rad.nextBoolean() ? "good" : "bad";
            transTotal = randomFieldGenerator.getRandomInteger(10,1000);
            // adding record into list
            writer.write(custID + "," +transID+ "," + transNumItems + "," + transDesc + ","+ transTotal +"\n");
        }
        writer.close();
    }

    // method to create records for CustomerSkewed
    public void createData(int numRecords, String filepath) throws IOException {
        //customer: ID,Name,Age,gender,countryCode,salary
        int ID, age, countryCode;
        float salary;
        String name, gender;
        // define range of lengths for each field
        int min_name = 10, max_name = 20, min_age = 10, max_age = 70;
        BufferedWriter writer = new BufferedWriter(new FileWriter(filepath));
        for (int i = 1; i <= numRecords; i++) {
            // ID in range from 1 to total number of records
            ID = i;
            // generating random string Name of length 10-20
            name = generateRandomString(rad.nextInt(max_name - min_name + 1) + min_name);
            // generating random Age from range 10-70
            age = rad.nextInt(max_age - min_age + 1) + min_age;
            gender = rad.nextBoolean() ? "male" : "female";
            countryCode = randomFieldGenerator.getRandomInteger(1, 10);
            salary = randomFieldGenerator.getRandomFloat(100, 10000);
            // adding record into list
            writer.write(ID + "," + name + "," + age + "," + gender + "," + countryCode + "," + salary + "\n");
        }
        writer.close();
    }

    // method to generate random string
    private String generateRandomString(int len) {
        String alpha = "abcdefghijklmnopqrstuvwxyz";
        // concatenating all characters
        String allchars = alpha + alpha.toUpperCase() + "0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            // randomly selecting characters and building string
            sb.append(allchars.charAt(rad.nextInt(allchars.length())));
        }

        return sb.toString();
    }

    // method to write records in file
    public void writeToFile(String filename, List<String> lines) {
        BufferedWriter bw = null;
        try {
            // writing records
            bw = new BufferedWriter(new FileWriter(filename));
            for (String record : lines) {
                bw.write(record);
            }

            bw.close();

        } catch (Exception e) {
            System.err.println("Error writing the file : ");
            e.printStackTrace();
        }
    }

    // main
    public static void main(String[] args) throws IOException {
        SkewedDataGenerator s = new SkewedDataGenerator();
        // Create a Scanner object

        Scanner sc = new Scanner(System.in);

        System.out.println("Enter file size in bytes(integer value): ");
        int fileSize = sc.nextInt();

        System.out.println("Enter Skewness (between 0-1 float value): ");
        float skew = sc.nextFloat();

        System.out.println("Enter Number of keys to be skewed (int): ");
        int numSkewKey = sc.nextInt();

        System.out.println("Creating Skewed Dataset TransactionSkewed..");
        // creating records - assuming one record around 25 bytes in A
        // Dataset(Transaction)
        s.createSkewedData(fileSize / 25, skew, numSkewKey, "TransactionSkewed.txt"); // passing approx number of records,
        System.out.println("Skewed Dataset TransactionSkewed created!");

        System.out.println("Creating Dataset CustomerSkewed..");
        // creating records - asssuming one record around 25 bytes in B
        // Dataset(Customer)
        s.createData(fileSize / 25, "CustomerSkewed.txt"); // passing approx number of records
        System.out.println("Dataset CustomerSkewed created!");
        sc.close();
    }
}
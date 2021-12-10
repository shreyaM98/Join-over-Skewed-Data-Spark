import java.util.Random;
import java.util.Scanner;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;

public class SkewedDataGenerator {
    Random rad = new Random();
    private RandomFieldGenerator randomFieldGenerator;

    // create records for Customer dataset
    public void customerData(int numRecords, String filepath) throws IOException {
        int ID, age, countryCode;
        float salary;
        String name, gender;
        BufferedWriter writer = new BufferedWriter(new FileWriter(filepath));
        for (int i = 1; i <= numRecords; i++) {
            ID = i;
            name = randomFieldGenerator.getRandomString(10,20);
            age = randomFieldGenerator.getRandomInteger(10,70);
            gender = randomFieldGenerator.generateRandomGender();
            countryCode = randomFieldGenerator.getRandomInteger(1, 10);
            salary = randomFieldGenerator.getRandomFloat(100, 10000);
            writer.write(ID + "," + name + "," + age + "," + gender + "," + countryCode + "," + salary + "\n");
        }
        writer.close();
    }

    //creating skewed data - transaction
    public void transactionSkewed(int numRecords, float skew, int numSkewKey, String filepath) throws IOException {
        int custID, transNumItems, transID, transTotal;
        String transDesc;
        randomFieldGenerator = new RandomFieldGenerator();
        BufferedWriter writer = new BufferedWriter(new FileWriter(filepath));
        int lastTransId = 0;
        //generating rows with skewed keys
        for (int i = 0; i < skew * numRecords; i++) {
            transID = i;
            lastTransId=transID;
            custID = rad.nextInt(numSkewKey) + 1;
            transNumItems = randomFieldGenerator.getRandomInteger(1,10);
            transDesc = randomFieldGenerator.getRandomString(20,50);
            transTotal = randomFieldGenerator.getRandomInteger(10,1000);
            writer.write(custID + "," +transID+ "," + transNumItems + "," + transDesc + ","+ transTotal +"\n");
        }
        lastTransId++;

        //non-skewed data
        for (int i = 0; i < (numRecords - skew * numRecords); i++) {
            custID = rad.nextInt(numRecords - numSkewKey + 1) + numSkewKey;
            transID = lastTransId + i;
            transNumItems = randomFieldGenerator.getRandomInteger(1,10);
            transDesc = randomFieldGenerator.getRandomString(20,50);
            transTotal = randomFieldGenerator.getRandomInteger(10,1000);
            writer.write(custID + "," +transID+ "," + transNumItems + "," + transDesc + ","+ transTotal +"\n");
        }
        writer.close();
    }

    // main
    public static void main(String[] args) throws IOException {
        SkewedDataGenerator s = new SkewedDataGenerator();
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter number of Records for Customer file: ");
        int numRec = sc.nextInt();
        System.out.println("Enter Skewness (float value between 0-1): ");
        float skew = sc.nextFloat();
        System.out.println("Enter Number of keys to be skewed (int): ");
        int numSkewKey = sc.nextInt();
        s.transactionSkewed((int) (numRec * 2.643), skew, numSkewKey, "TransactionSkewed.txt");
        System.out.println("Skewed Dataset TransactionSkewed created!\n");

        s.customerData(numRec, "Customer.txt");
        System.out.println("Customer Dataset created!");
        sc.close();
    }
}
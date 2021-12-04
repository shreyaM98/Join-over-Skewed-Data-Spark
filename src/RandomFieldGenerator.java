import java.util.concurrent.ThreadLocalRandom;

public class RandomFieldGenerator {

    private static final String CHAR_LIST =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    public float getRandomFloat(float min, float max) {
        return ThreadLocalRandom.current().nextFloat() * (max - min) + min;
    }

    public String getRandomString(int minLength, int maxLength) {
        int randomStringLength = getRandomInteger(minLength, maxLength);
        StringBuilder randomString = new StringBuilder();
        for(int i=0; i<randomStringLength; i++){
            int number = getRandomInteger(0, CHAR_LIST.length() - 1);
            char character = CHAR_LIST.charAt(number);
            randomString.append(character);
        }
        return randomString.toString();
    }

    public int getRandomInteger(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    public String generateRandomGender() {
        if (ThreadLocalRandom.current().nextDouble()*2 < 1) { return "female"; }
        else { return "male"; }
    }

    public int getRandomGaussian(int mean, int standardDeviation) {
        double gaussian = ThreadLocalRandom.current().nextGaussian() * standardDeviation + mean;
        return (int) gaussian;
    }
}
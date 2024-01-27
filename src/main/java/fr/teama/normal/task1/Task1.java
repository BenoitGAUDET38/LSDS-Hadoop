package fr.teama.normal.task1;

import fr.teama.normal.task1.usermovie.Job1;
import fr.teama.normal.task1.usermoviename.Job2;

import java.io.IOException;

public class Task1 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("\u001B[34mJOB 1 : Get favourite movie ID per user ID\u001B[0m");
        if (Job1.execute() == 1) {
            System.exit(1);
        }
        System.out.println("\u001B[34mJOB 2 : Replace movie ID by movie name\u001B[0m");
        if (Job2.execute() == 1) {
            System.exit(1);
        }

        System.exit(0);
    }
}

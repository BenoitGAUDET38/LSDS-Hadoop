package fr.teama.normal.task2;

import fr.teama.normal.task2.countmovie.Job3;
import fr.teama.normal.task2.groupmovie.Job4;

import java.io.IOException;

public class Task2 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("\u001B[34mJOB 1 : Count the number of favourite user movie\u001B[0m");
        if (Job3.execute() == 1) {
            System.exit(1);
        }
        System.out.println("\u001B[34mJOB 1 : Group the movies that have the same count\u001B[0m");
        if (Job4.execute() == 1) {
            System.exit(1);
        }

        System.exit(0);
    }
}

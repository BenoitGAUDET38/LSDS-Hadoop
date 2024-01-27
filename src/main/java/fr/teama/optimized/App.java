package fr.teama.optimized;

import fr.teama.optimized.groupmovie.Job3;
import fr.teama.optimized.moviecount.Job2;
import fr.teama.optimized.usermovie.Job1;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("\u001B[34mJOB 1 : Get favourite movieId per userId\u001B[0m");
        if (Job1.execute() == 1) {
            System.exit(1);
        }
        System.out.println("\u001B[34mJOB 2 : Count the number of users that have a movie has their favourite movie\u001B[0m");
        if (Job2.execute() == 1) {
            System.exit(1);
        }
        System.out.println("\u001B[34mJOB 3 : Group per movie count\u001B[0m");
        if (Job3.execute() == 1) {
            System.exit(1);
        }

        System.exit(0);
    }
}

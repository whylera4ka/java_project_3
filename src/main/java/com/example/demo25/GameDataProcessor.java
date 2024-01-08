package com.example.demo25;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GameDataProcessor {
    private static final String CSV_FILE_PATH = "C:\\Users\\Пользователь\\Desktop\\12\\src\\main\\java\\com\\example\\demo25\\game.csv";
    private static final String DB_URL = "jdbc:sqlite:game_data.db";

    public static void main(String[] args) {
        // Создание базы данных и таблиц
        createDatabaseAndTables();
        // Чтение данных из CSV-файла и сохранение в базу данных
        readCSVAndSaveToDatabase();
        // Выполнение SQL-запросов
        executeSQLQueries();
    }

    private static void createDatabaseAndTables() {
        try {
            // Подключение к базе данных
            Connection connection = DriverManager.getConnection(DB_URL);
            // Создание таблицы games
            String createGamesTableQuery = "CREATE TABLE IF NOT EXISTS games ("
                    + "rank INTEGER, "
                    + "name TEXT, "
                    + "platform TEXT, "
                    + "year INTEGER, "
                    + "genre TEXT, "
                    + "publisher TEXT, "
                    + "na_sales REAL, "
                    + "eu_sales REAL, "
                    + "jp_sales REAL, "
                    + "other_sales REAL, "
                    + "global_sales REAL)";
            connection.createStatement().executeUpdate(createGamesTableQuery);
            // Закрытие ресурсов
            connection.close();
        } catch (SQLException e) {

        }
    }

    private static void readCSVAndSaveToDatabase() {
        try {
            // Подключение к базе данных
            Connection connection = DriverManager.getConnection(DB_URL);
            // Чтение данных из CSV-файла
            BufferedReader reader = new BufferedReader(new FileReader(CSV_FILE_PATH));
            reader.readLine(); // Пропуск первой строки с заголовками
            int insertedRows = 0;
            boolean waitForMessageDisplayed = false;

            String line;
            while ((line = reader.readLine()) != null) {
                // Разделение строки по запятой
                String[] data = line.split(",");
                // Вставка данных в таблицу
                String insertQuery = "INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                if (!waitForMessageDisplayed) {
                    System.out.println("ОЖИДАЙТЕ");
                    waitForMessageDisplayed = true;
                }
                try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
                    statement.setInt(1, Integer.parseInt(data[0])); // rank
                    statement.setString(2, data[1]); // name
                    statement.setString(3, data[2]); // platform
                    statement.setInt(4, Integer.parseInt(data[3])); // year
                    statement.setString(5, data[4]); // genre
                    statement.setString(6, data[5]); // publisher
                    statement.setDouble(7, parseDoubleWithDefault(data[6], 0.0)); // na_sales
                    statement.setDouble(8, parseDoubleWithDefault(data[7], 0.0)); // eu_sales
                    statement.setDouble(9, parseDoubleWithDefault(data[8], 0.0)); // jp_sales
                    statement.setDouble(10, parseDoubleWithDefault(data[9], 0.0)); // other_sales
                    statement.setDouble(11, parseDoubleWithDefault(data[10], 0.0)); // global_sales
                    statement.executeUpdate();
                    insertedRows++;
                } catch (SQLException | NumberFormatException e) {

                }
            }
            // Закрытие ресурсов
            reader.close();
            connection.close();
            System.out.println("Inserted " + insertedRows + " rows into the database.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void executeSQLQueries() {
        try {
            // Подключение к базе данных
            Connection connection = DriverManager.getConnection(DB_URL);
            // Запрос 1: Построение графика по средним показателям продаж по платформам
            String query1 = "SELECT platform, AVG(global_sales) AS average_sales "
                    + "FROM games "
                    + "GROUP BY platform";
            ResultSet result1 = connection.createStatement().executeQuery(query1);
            while (result1.next()) {
                String platform = result1.getString("platform");
                double averageSales = result1.getDouble("average_sales");
                System.out.println("Platform: " + platform + ", Average Sales: " + averageSales);
            }
            result1.close();
            // Запрос 2: Вывод игры с самым высоким показателем в Европе за 2000 год
            String query2 = "SELECT name "
                    + "FROM games "
                    + "WHERE year = 2000 "
                    + "ORDER BY eu_sales DESC "
                    + "LIMIT 1";
            ResultSet result2 = connection.createStatement().executeQuery(query2);
            if (result2.next()) {
                String gameName = result2.getString("name");
                System.out.println("Game with highest sales in Europe in 2000: " + gameName);
            }
            result2.close();
            // Запрос 3: Вывод игры с самым высоким показателем продаж в Японии из жанра спортивных игр
            String query3 = "SELECT name "
                    + "FROM games "
                    + "WHERE genre = 'Sports' "
                    + "AND year BETWEEN 2000 AND 2006 "
                    + "ORDER BY jp_sales DESC "
                    + "LIMIT 1";
            ResultSet result3 = connection.createStatement().executeQuery(query3);
            if (result3.next()) {
                String gameName = result3.getString("name");
                System.out.println("Game with highest sales in Japan from 2000 to 2006 in the Sports genre: " + gameName);
            }
            result3.close();
            // Закрытие ресурсов
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static double parseDoubleWithDefault(String str, double defaultValue) {
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;

public class RenameAccount {

    private static final String CONFIG_PATH = "./../../config/server.properties";

    private static String makeMD5(String input) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] hash = md.digest(input.getBytes("UTF-8"));
        return bytesToHex(hash);
    }

    private static String makeSHA256(String input) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hash = md.digest(input.getBytes("UTF-8"));
        return bytesToHex(hash);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b & 0xff));
        return sb.toString();
    }

    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            File file = new File(CONFIG_PATH);
            if (!file.exists()) {
                System.out.println("Cannot find " + CONFIG_PATH);
                return;
            }
            try (InputStream is = new FileInputStream(file)) {
                props.load(is);
            }

            String dbUrl = props.getProperty("URL");
            String dbUser = props.getProperty("Login");
            String dbPass = props.getProperty("Password");
            String salt = props.getProperty("PasswordSalt", "");

            Scanner scanner = new Scanner(System.in);
            String oldAcc, newAcc, password, mode;

            if (args.length >= 3) {
                oldAcc = args[0].toLowerCase();
                newAcc = args[1].toLowerCase();
                password = args[2];
                mode = args.length >= 4 ? args[3].toLowerCase() : "read";
            } else {
                System.out.print("Old account: ");
                oldAcc = scanner.nextLine().trim().toLowerCase();

                System.out.print("New account: ");
                newAcc = scanner.nextLine().trim().toLowerCase();

                System.out.print("Password: ");
                password = scanner.nextLine().trim();

                System.out.print("Mode (read/write/hash): ");
                mode = scanner.nextLine().trim().toLowerCase();
                if (mode.isEmpty()) mode = "read";
            }

            String hash = makeSHA256(salt + password + makeMD5(newAcc));

            if (mode.equals("hash")) {
                System.out.println("Password hash: " + hash);
                return;
            }

            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
                 Statement stmt = conn.createStatement()) {

                List<String> updates = new ArrayList<>();

                updates.add(String.format(
                        "UPDATE accounts SET login='%s', password='%s' WHERE login='%s';",
                        newAcc, hash, oldAcc
                ));

                updates.add(String.format(
                        "UPDATE characters SET account_name='%s' WHERE account_name='%s';",
                        newAcc, oldAcc
                ));

                String infoSchemaQuery =
                        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_SCHEMA = DATABASE() " +
                                "AND TABLE_NAME NOT IN ('accounts','characters') " +
                                "AND (COLUMN_NAME LIKE '%account%' OR COLUMN_NAME LIKE '%login%') " +
                                "AND DATA_TYPE IN ('char','varchar','tinytext','text','mediumtext','longtext');";

                ResultSet rs = stmt.executeQuery(infoSchemaQuery);
                while (rs.next()) {
                    String table = rs.getString("TABLE_NAME");
                    String column = rs.getString("COLUMN_NAME");
                    updates.add(String.format(
                            "UPDATE `%s` SET `%s`='%s' WHERE `%s`='%s';",
                            table, column, newAcc, column, oldAcc
                    ));
                }

                System.out.println("\nSQL Statements to execute (" + updates.size() + " total):");
                for (String sql : updates) System.out.println(sql);

                if (mode.equals("read")) {
                    System.out.println("\nRead mode only - no database changes made.");
                    return;
                }

                System.out.print("\nConfirm write to database? (yes/no): ");
                String confirm = scanner.nextLine().trim().toLowerCase();
                if (!confirm.equals("yes")) {
                    System.out.println("Aborted.");
                    return;
                }

                int success = 0;
                for (String sql : updates) {
                    try {
                        stmt.executeUpdate(sql);
                        success++;
                    } catch (SQLException ex) {
                        System.out.println("Error executing: " + sql);
                        System.out.println("   " + ex.getMessage());
                    }
                }

                System.out.println("\nDone. " + success + " / " + updates.size() + " statements executed successfully.");
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("\nError: " + e.getMessage());
        }
    }
}

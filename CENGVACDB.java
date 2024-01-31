package ceng.ceng351.cengvacdb;

import java.sql.*;
import java.util.ArrayList;

public class CENGVACDB implements ICENGVACDB {

    private static Connection connection = null;

    @Override
    public void initialize() {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://144.122.71.121:8080/db2235521?useSSL=false", "e2235521", "7AnIdBe9gaRo");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int createTables() {
        int numberOfTables = 0;
        String createUser = "CREATE TABLE User (" +
                "userID INT NOT NULL, " +
                "userName VARCHAR(30), " +
                "age INT, " +
                "address VARCHAR(150), " +
                "password VARCHAR(30), " +
                "status VARCHAR(15), " +
                "PRIMARY KEY (userID));";
        String createVaccine = "CREATE TABLE Vaccine(" +
                "code INT NOT NULL, " +
                "vaccinename VARCHAR(30), " +
                "type VARCHAR(30), " +
                "PRIMARY KEY (code));";
        String createVaccination = "CREATE TABLE Vaccination(" +
                "code INT NOT NULL, " +
                "userID INT NOT NULL, " +
                "dose INT NOT NULL, " +
                "vacdate DATE, " +
                "PRIMARY KEY (code, userID, dose), " +
                "FOREIGN KEY (code) REFERENCES Vaccine(code) ON DELETE CASCADE, " +
                "FOREIGN KEY (userID) REFERENCES User(userID));";
        String createAllergicSideEffect = "CREATE TABLE AllergicSideEffect(" +
                "effectcode INT NOT NULL, " +
                "effectname VARCHAR(50), " +
                "PRIMARY KEY(effectcode));";
        String createSeen = "CREATE TABLE Seen(" +
                "effectcode INT NOT NULL, " +
                "code INT NOT NULL, " +
                "userID INT NOT NULL, " +
                "date DATE, " +
                "degree VARCHAR(30), " +
                "PRIMARY KEY(effectcode, code, userID), " +
                "FOREIGN KEY(effectcode) REFERENCES AllergicSideEffect(effectcode), " +
                "FOREIGN KEY(code) REFERENCES Vaccination(code) ON DELETE CASCADE, " +
                "FOREIGN KEY(userID) REFERENCES User(userID));";

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createUser);
            numberOfTables++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createVaccine);
            numberOfTables++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createVaccination);
            numberOfTables++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createAllergicSideEffect);
            numberOfTables++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(createSeen);
            numberOfTables++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numberOfTables;
    }

    @Override
    public int dropTables() {
        int numberOfTablesD = 0;
        String dropUser = "DROP TABLE IF EXISTS User;";
        String dropVaccine = "DROP TABLE IF EXISTS Vaccine;";
        String dropVaccination = "DROP TABLE IF EXISTS Vaccination;";
        String dropAllergicSideEffect = "DROP TABLE IF EXISTS AllergicSideEffect;";
        String dropSeen = "DROP TABLE IF EXISTS Seen;";

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(dropSeen);
            numberOfTablesD++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(dropAllergicSideEffect);
            numberOfTablesD++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(dropVaccination);
            numberOfTablesD++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(dropVaccine);
            numberOfTablesD++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(dropUser);
            numberOfTablesD++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return numberOfTablesD;
    }

    @Override
    public int insertUser(User[] users) {
        int numberOfRowsU = 0;
        String insertU = "INSERT INTO User(" +
                "userID, " +
                "userName, " +
                "age, " +
                "address, " +
                "password, " +
                "status) VALUES (?, ?, ?, ?, ?, ?);";
        for(int i = 0; i < users.length; i++) {
            User user = users[i];
            try {
                PreparedStatement pstatement = this.connection.prepareStatement(insertU);
                pstatement.setInt(1, user.getUserID());
                pstatement.setString(2, user.getUserName());
                pstatement.setInt(3, user.getAge());
                pstatement.setString(4, user.getAddress());
                pstatement.setString(5, user.getPassword());
                pstatement.setString(6, user.getStatus());
                pstatement.executeUpdate();
                numberOfRowsU++;
                pstatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return numberOfRowsU;
    }

    @Override
    public int insertAllergicSideEffect(AllergicSideEffect[] sideEffects) {
        int numberOfRowsA = 0;
        String insertASE = "INSERT INTO AllergicSideEffect(" +
                "effectcode, " +
                "effectname) VALUES (?, ?);";
        for(int i = 0; i < sideEffects.length; i++) {
            AllergicSideEffect ASE = sideEffects[i];
            try {
                PreparedStatement pstatement = this.connection.prepareStatement(insertASE);
                pstatement.setInt(1, ASE.getEffectCode());
                pstatement.setString(2, ASE.getEffectName());
                pstatement.executeUpdate();
                numberOfRowsA++;
                pstatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return numberOfRowsA;
    }

    @Override
    public int insertVaccine(Vaccine[] vaccines) {
        int numberOfRowsV = 0;
        String insertV = "INSERT INTO Vaccine(" +
                "code, " +
                "vaccinename, " +
                "type) VALUES (?, ?, ?);";
        for(int i = 0; i < vaccines.length; i++) {
            Vaccine vac = vaccines[i];
            try {
                PreparedStatement pstatement = this.connection.prepareStatement(insertV);
                pstatement.setInt(1, vac.getCode());
                pstatement.setString(2, vac.getVaccineName());
                pstatement.setString(3, vac.getType());
                pstatement.executeUpdate();
                numberOfRowsV++;
                pstatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return numberOfRowsV;
    }

    @Override
    public int insertVaccination(Vaccination[] vaccinations) {
        int numberOfRowsV = 0;
        String insertV = "INSERT INTO Vaccination(" +
                "code, " +
                "userID, " +
                "dose, " +
                "vacdate) VALUES (?, ?, ?, ?);";
        for(int i = 0; i < vaccinations.length; i++) {
            Vaccination vac = vaccinations[i];
            try {
                PreparedStatement pstatement = this.connection.prepareStatement(insertV);
                pstatement.setInt(1, vac.getCode());
                pstatement.setInt(2, vac.getUserID());
                pstatement.setInt(3, vac.getDose());
                pstatement.setString(4, vac.getVacdate());
                pstatement.executeUpdate();
                numberOfRowsV++;
                pstatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return numberOfRowsV;
    }

    @Override
    public int insertSeen(Seen[] seens) {
        int numberOfRowsS = 0;
        String insertS = "INSERT INTO Seen(" +
                "effectcode, " +
                "code, " +
                "userID, " +
                "date, " +
                "degree) VALUES (?, ?, ?, ?, ?);";
        for(int i = 0; i < seens.length; i++) {
            Seen sn = seens[i];
            try {
                PreparedStatement pstatement = this.connection.prepareStatement(insertS);
                pstatement.setInt(1, sn.getEffectcode());
                pstatement.setInt(2, sn.getCode());
                pstatement.setInt(3, sn.getUserID());
                pstatement.setString(4, sn.getDate());
                pstatement.setString(5, sn.getDegree());
                pstatement.executeUpdate();
                numberOfRowsS++;
                pstatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return numberOfRowsS;
    }

    @Override
    public Vaccine[] getVaccinesNotAppliedAnyUser() {
        ArrayList<Vaccine> temp = new ArrayList<Vaccine>();
        ResultSet rSet;
        String query1 = "SELECT DISTINCT V.code, V.vaccinename, V.type " +
                "FROM Vaccine V " +
                "WHERE V.code NOT IN (SELECT V1.code FROM Vaccination V1) " +
                "ORDER BY V.code ASC;";

        try {
            Statement statement = this.connection.createStatement();
            rSet = statement.executeQuery(query1);
            while(rSet.next()) {
                int code = rSet.getInt("code");
                String name = rSet.getString("vaccinename");
                String type = rSet.getString("type");
                temp.add(new Vaccine(code, name, type));
            }
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Vaccine[] vaccines = new Vaccine[temp.size()];
        for(int i = 0; i < temp.size(); i++) {
            vaccines[i] = temp.get(i);
        }
        return vaccines;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersforTwoDosesByDate(String vacdate) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> temp = new ArrayList<>();
        ResultSet rSet;
        String query2 = "SELECT DISTINCT U.userID, U.userName, U.address " +
                "FROM User U, Vaccination V, Vaccination V1, Vaccination V2 " +
                "WHERE U.userID = V.userID AND U.userID = V1.userID AND U.userID = V2.userID AND V.vacdate >= ? AND V1.vacdate >= ? " +
                "AND V2.vacdate >= ? AND (V1.dose - V.dose = 1) AND V2.dose >= V1.dose AND " +
                "V2.userID NOT IN (SELECT V3.userID FROM Vaccination V3 WHERE V3.dose - V.dose >= 2) " +
                "ORDER BY U.userID ASC;";
        try {
            PreparedStatement pstatement = this.connection.prepareStatement(query2);
            pstatement.setString(1, vacdate);
            pstatement.setString(2, vacdate);
            pstatement.setString(3, vacdate);
            rSet = pstatement.executeQuery();
            while(rSet.next()) {
                int userID = rSet.getInt("userID");
                String name = rSet.getString("userName");
                String address = rSet.getString("address");
                temp.add(new QueryResult.UserIDuserNameAddressResult(userID, name, address));
            }
            pstatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        QueryResult.UserIDuserNameAddressResult[] users = new QueryResult.UserIDuserNameAddressResult[temp.size()];
        for(int i = 0; i < temp.size(); i++) {
            users[i] = temp.get(i);
        }
        return users;
    }

    @Override
    public Vaccine[] getTwoRecentVaccinesDoNotContainVac() {
        ArrayList<Vaccine> temp = new ArrayList<Vaccine>();
        ResultSet rSet;
        String query3 = "SELECT DISTINCT V.code, V.vaccinename, V.type, C.vacdate " +
                 "FROM Vaccine V, Vaccination C " +
                "WHERE V.code = C.code AND V.vaccinename NOT LIKE '%vac%' AND C.vacdate = (SELECT MAX(C1.vacdate) FROM Vaccination C1 WHERE C1.code = V.code) " +
                "ORDER BY C.vacdate DESC, V.code ASC " +
                "LIMIT 2;";

        try {
            Statement statement = this.connection.createStatement();
            rSet = statement.executeQuery(query3);
            while(rSet.next()) {
                int code = rSet.getInt("code");
                String name = rSet.getString("vaccinename");
                String type = rSet.getString("type");
                temp.add(new Vaccine(code, name, type));
            }
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Vaccine[] vaccines = new Vaccine[temp.size()];
        for(int i = 0; i < temp.size(); i++) {
            vaccines[i] = temp.get(i);
        }
        return vaccines;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersAtHasLeastTwoDoseAtMostOneSideEffect() {
        ArrayList<QueryResult.UserIDuserNameAddressResult> temp = new ArrayList<>();
        ResultSet rSet;
        String query4 = "SELECT DISTINCT U.userID, U.userName, U.address " +
                "FROM User U, Vaccination V " +
                "WHERE U.userID = V.userID AND V.dose >= 2 AND U.userID NOT IN " +
                "(SELECT U1.userID FROM User U1, Seen S1, Seen S2 WHERE U1.userID = S1.userID AND U1.userID = S2.userID AND S1.effectcode <> S2.effectcode) " +
                "ORDER BY U.userID ASC;";
        try {
            PreparedStatement pstatement = this.connection.prepareStatement(query4);
            rSet = pstatement.executeQuery();
            while(rSet.next()) {
                int userID = rSet.getInt("userID");
                String name = rSet.getString("userName");
                String address = rSet.getString("address");
                temp.add(new QueryResult.UserIDuserNameAddressResult(userID, name, address));
            }
            pstatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        QueryResult.UserIDuserNameAddressResult[] users = new QueryResult.UserIDuserNameAddressResult[temp.size()];
        for(int i = 0; i < temp.size(); i++) {
            users[i] = temp.get(i);
        }
        return users;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getVaccinatedUsersWithAllVaccinesCanCauseGivenSideEffect(String effectname) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> temp = new ArrayList<>();
        ResultSet rSet;
        String query5 = "SELECT DISTINCT U.userID, U.userName, U.address " +
                "FROM User U WHERE NOT EXISTS " +
                "(SELECT V.code FROM Vaccine V, AllergicSideEffect ASE, Seen S " +
                "WHERE S.code = V.code AND ASE.effectname = ? AND ASE.effectcode = S.effectcode AND " +
                "NOT EXISTS (SELECT V1.code FROM Vaccination V1, Vaccine V, AllergicSideEffect ASE, Seen S " +
                "WHERE S.code = V.code AND ASE.effectname = ? AND ASE.effectcode = S.effectcode AND V1.code = V.code AND V1.userID = U.userID)) " +
                "ORDER BY U.userID ASC;";
        try {
            PreparedStatement pstatement = this.connection.prepareStatement(query5);
            pstatement.setString(1, effectname);
            pstatement.setString(2, effectname);
            rSet = pstatement.executeQuery();
            while(rSet.next()) {
                int userID = rSet.getInt("userID");
                String name = rSet.getString("userName");
                String address = rSet.getString("address");
                temp.add(new QueryResult.UserIDuserNameAddressResult(userID, name, address));
            }
            pstatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        QueryResult.UserIDuserNameAddressResult[] users = new QueryResult.UserIDuserNameAddressResult[temp.size()];
        for(int i = 0; i < temp.size(); i++) {
            users[i] = temp.get(i);
        }
        return users;
    }

    @Override
    public QueryResult.UserIDuserNameAddressResult[] getUsersWithAtLeastTwoDifferentVaccineTypeByGivenInterval(String startdate, String enddate) {
        ArrayList<QueryResult.UserIDuserNameAddressResult> temp = new ArrayList<>();
        ResultSet rSet;
        String query6 = "SELECT DISTINCT U.userID, U.userName, U.address " +
                "FROM User U, Vaccination V1, Vaccination V2 " +
                "WHERE U.userID = V1.userID AND U.userID = V2.userID AND V1.code <> V2.code AND " +
                "V1.vacdate >= ? AND V1.vacdate <= ? AND V2.vacdate >= ? AND V2.vacdate <= ? " +
                "ORDER BY U.userID ASC;";
        try {
            PreparedStatement pstatement = this.connection.prepareStatement(query6);
            pstatement.setString(1, startdate);
            pstatement.setString(2, enddate);
            pstatement.setString(3, startdate);
            pstatement.setString(4, enddate);
            rSet = pstatement.executeQuery();
            while(rSet.next()) {
                int userID = rSet.getInt("userID");
                String name = rSet.getString("userName");
                String address = rSet.getString("address");
                temp.add(new QueryResult.UserIDuserNameAddressResult(userID, name, address));
            }
            pstatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        QueryResult.UserIDuserNameAddressResult[] users = new QueryResult.UserIDuserNameAddressResult[temp.size()];
        for(int i = 0; i < temp.size(); i++) {
            users[i] = temp.get(i);
        }
        return users;
    }

    @Override
    public AllergicSideEffect[] getSideEffectsOfUserWhoHaveTwoDosesInLessThanTwentyDays() {
        ArrayList<AllergicSideEffect> temp = new ArrayList<AllergicSideEffect>();
        ResultSet rSet;
        String query7 = "SELECT DISTINCT ASE.effectcode, ASE. effectname " +
                "FROM AllergicSideEffect ASE, Seen S, Vaccination V1, Vaccination V2 " +
                "WHERE ASE.effectcode = S.effectcode AND V2.userID = V1.userID AND S.userID = V2.userID AND " +
                "V1.dose = 2 AND V2.dose = 1 AND DATEDIFF(V1.vacdate, V2.vacdate) < 20 " +
                "ORDER BY ASE.effectcode ASC;";
        try {
            Statement statement = this.connection.createStatement();
            rSet = statement.executeQuery(query7);
            while(rSet.next()) {
                int code = rSet.getInt("effectcode");
                String name = rSet.getString("effectname");
                temp.add(new AllergicSideEffect(code, name));
            }
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        AllergicSideEffect[] ases = new AllergicSideEffect[temp.size()];
        for(int i = 0; i < temp.size(); i++) {
            ases[i] = temp.get(i);
        }
        return ases;
    }

    @Override
    public double averageNumberofDosesofVaccinatedUserOverSixtyFiveYearsOld() {

        String query8 = "SELECT AVG(V.dose) " +
                "FROM Vaccination V, User U " +
                "WHERE U.userID = V.userID AND U.age > 65 AND V.vacdate = (SELECT MAX(V.vacdate) FROM Vaccination V WHERE V.userID = U.userID);";
        double avg = 0;
        ResultSet rSet;
        try {
            PreparedStatement statement = this.connection.prepareStatement(query8);
            rSet = statement.executeQuery();
            rSet.next();
            avg = rSet.getDouble("AVG(V.dose)");
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return avg;
    }

    @Override
    public int updateStatusToEligible(String givendate) {
        String query9 = "UPDATE User U " +
                "SET U.status = 'eligible' " +
                "WHERE U.status <> 'eligible' AND U.userID IN (SELECT V.userID FROM Vaccination V WHERE V.vacdate = (SELECT MAX(V.vacdate) FROM Vaccination V WHERE V.userID = U.userID) AND DATEDIFF(?, V.vacdate) >= 120);";
        int numberOfUpdates = 0;
        try {
            PreparedStatement statement = this.connection.prepareStatement(query9);
            statement.setString(1, givendate);
            numberOfUpdates = statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return numberOfUpdates;
    }

    @Override
    public Vaccine deleteVaccine(String vaccineName) {
        Vaccine v = new Vaccine(0, null, null);
        ResultSet rSet;
        String query10 = "SELECT DISTINCT V.code, V.vaccinename, V.type " +
                "FROM Vaccine V " +
                "WHERE V.vaccinename = ?;";
        String query11 = "DELETE FROM Vaccine V " +
                "WHERE V.vaccinename = ?;";
        try {
            PreparedStatement statement = this.connection.prepareStatement(query10);
            statement.setString(1, vaccineName);
            rSet = statement.executeQuery();
            rSet.next();
            v.setCode(rSet.getInt("V.code"));
            v.setVaccineName(rSet.getString("V.vaccinename"));
            v.setType(rSet.getString("V.type"));
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            PreparedStatement pstatement = this.connection.prepareStatement(query11);
            pstatement.setString(1, vaccineName);
            pstatement.executeUpdate();
            pstatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return v;
    }
}

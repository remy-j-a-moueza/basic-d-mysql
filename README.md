basic-d-mysql

A dead simple and basic module to access MySQL databases using the C API. 
Row values are returned as strings.

```d
import connection;

/// Connect to a database. 
auto con = new Connection ().connect ("localhost", 
                                      "user-name",
                                      "password",
                                      "database");

/// Execute statements, without retrieving the results.
con.execute (`INSERT INTO STUFF( ID, VAL, THING)
                     VALUES (1, 'val', 'thing')`);

/// Retrieve the last inserted ID.
ulong lastID = con.lastInsertId ();

/// Retrieve results as an associative array.
string [string] rowsAA = con.query (`SELECT * FROM STUFF`);

foreach (row; rowsAA) {
    writeln (row ["VAL"], ": ", row ["THING"]);
}

/// Retrieve values as a result set. More efficient memory wise.
ResultSet rowsRS = con.query (`SELECT * FROM STUFF`);

/// Iterates over the ResultSet as for an associative array.
foreach (row; rowsRS) {
    writeln (row ["VAL"], ": ", row ["THING"]);
}

/// Rewind the result set if you want to reuse it.
rowsRS.rewind ();
```

Compile your program using the mysqlclient library: 

```bash
    dmd -L-lmysqlclient <your_module.d> path/to/connection.d
```

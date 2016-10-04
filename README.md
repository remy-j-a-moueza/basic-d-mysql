# libDbNative

A dead simple and basic module to access MySQL and Sqliet databases using the C API. 
Row values are returned as strings.

## Examples

```d
import dbconnection;
import mysqlcon;

/// Connect to a database. 
auto con = new MySQL ("localhost", 
                      "user-name",
                      "password",
                      "database");
/* 
// Alternative: 
    auto con = new Sqlite ("/path/to/sqlite-database.db");
*/

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

## Compilation
It can be included as a `dub` dependency.

For **MySQL**, compile your program using the `mysqlclient` library: 
- add a `libs "mysqlclient"` to your `dub.sdl` file
- or `"libs": ["mysqlclient"]` to your `dub.json` file.

For **Sqlite**, compile your program using the `sqlite3` library: 
- add a `libs "sqlite3"` to your `dub.sdl` file
- or `"libs": ["sqlite3"]` to your `dub.json` file.

## Character encoding
The  `MySQL` and `Sqlite` implementation of `Connection` are templated: 
they take a string type as template parameter. 
That way, you can interact with a database using a character set other than `utf-8`, as long as that caracter set is supported in `D`.

```d
import dbconnection;
import mysqlcon;

/// Connect to a database, using a iso-8859-1 character set.
auto con = new MySQL!Latin1String (
                        "localhost", 
                        "user-name",
                        "password",
                        "database");
/* 
// Alternative: 
    auto con = new Sqlite!Latin1String ("/path/to/sqlite-database.db");
*/

/// Now, use the con instance as usual.
```

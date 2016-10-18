// vim: inde= cin cino=(0,w4,W0,u2,)0 ft=d
import std.stdio;
import std.string;
import std.conv;
import std.range;
import std.encoding;

import dbconnection;


/// This should not be necessary: use the etc.c.sqlite3 module; 
extern (C) {
    struct sqlite3;
    struct sqlite3_stmt;

    alias  long sqlite3_int64;
    alias ulong sqlite3_uint64;

    alias int function (void*, int, char**, char**) sqlite3_callback;

    int sqlite3_open (const(char)* filename, sqlite3** ppDb);
    
    int sqlite3_close (sqlite3*);

    int sqlite3_prepare_v2 (
            sqlite3* db, 
            const(char)* zSql, 
            int nByte, 
            sqlite3_stmt** ppStmt, 
            const(char*)* pzTail);

    int sqlite3_exec (
            sqlite3*,            /* An open database */
            const(char)* sql,    /* SQL to be evaluated */
                                 /* Callback function */
            int function (void*, int, char**, char**) callback,   
            void*,               /* 1st argument to callback */
            char** errmsg);      /* Error msg written here */

    void sqlite3_free (void*);

    sqlite3_int64 sqlite3_last_insert_rowid (sqlite3*);

    const(char)* sqlite3_errmsg (sqlite3*);

    enum SQLITE : int {

        OK         =    0,  /* Successful result */
        ERROR      =    1,  /* SQL error or missing database */
        INTERNAL   =    2,  /* Internal logic error in SQLite */
        PERM       =    3,  /* Access permission denied */
        ABORT      =    4,  /* Callback routine requested an abort */
        BUSY       =    5,  /* The database file is locked */
        LOCKED     =    6,  /* A table in the database is locked */
        NOMEM      =    7,  /* A malloc() failed */
        READONLY   =    8,  /* Attempt to write a readonly database */
        INTERRUPT  =    9,  /* Operation terminated by sqlite3_interrupt()*/
        IOERR      =   10,  /* Some kind of disk I/O error occurred */
        CORRUPT    =   11,  /* The database disk image is malformed */
        NOTFOUND   =   12,  /* Unknown opcode in sqlite3_file_control() */
        FULL       =   13,  /* Insertion failed because database is full */
        CANTOPEN   =   14,  /* Unable to open the database file */
        PROTOCOL   =   15,  /* Database lock protocol error */
        EMPTY      =   16,  /* Database is empty */
        SCHEMA     =   17,  /* The database schema changed */
        TOOBIG     =   18,  /* String or BLOB exceeds size limit */
        CONSTRAINT =   19,  /* Abort due to constraint violation */
        MISMATCH   =   20,  /* Data type mismatch */
        MISUSE     =   21,  /* Library used incorrectly */
        NOLFS      =   22,  /* Uses OS features not supported on host */
        AUTH       =   23,  /* Authorization denied */
        FORMAT     =   24,  /* Auxiliary database format error */
        RANGE      =   25,  /* 2nd parameter to sqlite3_bind out of range */
        NOTADB     =   26,  /* File opened that is not a database file */
        ROW        =  100,  /* sqlite3_step() has another row ready */
        DONE       =  101,  /* sqlite3_step() has finished executing */
    }
}

/** A connection to an Sqlite3 database.
 
 The template parameter value is the type of the database encoding. 
 All queries return strings in UTF-8 encoding. 
*/
class Sqlite (String = string) : Connection {

    protected {
        /// The connection to the database
        sqlite3 * con;

        /// The file name of the database we connected to.
        string _name;
    }

    this (string filename = ":memory:", 
          string file = __FILE__, int line = __LINE__)
    {

        if (sqlite3_open (filename.toStringz, &con)) {
            throw new Exception (
                    "Cannot open db \"%s\": %s".format (
                        filename,
                        sqlite3_errmsg (con).to!string
                    ), 
                    file, 
                    line
            );
            sqlite3_close (con);
        }
        _name = filename;
    }

    /// Return the filename we used for our initialization.
    string name () { return _name; }

    ~this () {
        close ();
    }

    /// Closes the connection.
    void close () {
        sqlite3_close (con);
    }
    
    /** Transcode a string from the database to UTF-8, according to our template
      parameter String.
    */
    string transcode (string src) {
        static if (is (String == string)) {
            /// if we are already using UTF-8, just return the string.
            return src;

        } else {
            String from = cast (String) src;
            string dest; 

            .transcode (from, dest);
            return dest;
        }
    }

    /// Retrieve the last inserted ID. 
    ulong lastInsertId () {
        return sqlite3_last_insert_rowid (con).to!ulong;
    }

    /// Executes a query, discarding the results.
    void execute (string command, 
                  string file = __FILE__, int line = __LINE__)
    {
        char * errMsg = null; 
        
        int ret = sqlite3_exec (con, command.toStringz, null, null, &errMsg);

        if (ret) {
            string err = errMsg.to!string.dup;
            sqlite3_free (errMsg);
            throw new Exception (err, file, line);
        }
    }

    /** A utility function to encode strings to the database encoding. */
    string toDbString (string src) {
        string res; 

        static if (is (String == string)) {
             return src;
        } else {
            String tmp;
            .transcode (src, tmp);
            return cast (string) tmp;
        }
    }

    /// Executes a query and retrieve its results.
    string [string][] query (string command, 
                             string file = __FILE__, int line = __LINE__) 
    {
        /* A class to hold the rows to have a reference semantic. 
           Otherwise we don't get any result at the end of the callback calls.
        */
        class Ctx {
            string [string][] rows;
            string delegate (string) transcode;

            this (typeof (transcode) dg) {
                this.transcode = dg;
            }
        }
        auto ctx = new Ctx (&transcode); 

        extern (C) int callback (
                        void * ptr,
                        int argc, 
                        char **argv, 
                        char **azColName) 
        {
            auto ctx  = cast (Ctx *) ptr;
            string [string]   row; 

            foreach (num; 0..argc) {
                string colName; 

                static if (is (String == string)) {
                    colName = azColName [num].to!string; 
                } else {
                    colName = ctx.transcode (azColName [num].to!string); 
                }

                if (argv [num]) {
                    static if (is (String == string)) {
                        row [colName] = argv [num].to!string;
                    } else {
                        row [colName] = ctx.transcode (argv [num].to!string);
                    }
                } else {
                    row [colName] = "";
                }
            }
            ctx.rows ~= row; 
            return 0;
        }

        char * errMsg = null; 
        string    cmd = toDbString (command);
        
        int ret = sqlite3_exec (
                        con, 
                        cmd.toStringz, 
                        &callback, 
                        cast (void *) &ctx, 
                        &errMsg);
        scope (exit) sqlite3_free (errMsg);

        if (ret) {
            string err = errMsg.to!string;
            throw new Exception (err, file, line);
        }

        return ctx.rows;
    }

    ResultSet select (string command, 
                             string file = __FILE__, int line = __LINE__)
    {
        /// A minimal implementation example: 
        version (None) {
            DbRow [] ls 
                = cast (DbRow []) 
                  this.query (command, file, line)
                      .map!(aa => new BasicDbRow (aa))
                      .array;
            
            return new BasicResultSet (inputRangeObject (ls));
        }
        
        SqliteResultSet res = new SqliteResultSet (&this.transcode); 

        extern (C) int callback (
                        void * ptr,
                        int argc, 
                        char **argv, 
                        char **azColName) 
        {
            auto res  = cast (SqliteResultSet *) ptr;
            string [string]   row; 

            if (res.keys.empty) {
                foreach (num; 0..argc) {
                    static if (is (String == string)) {
                        res.keys ~= azColName [num].to!string;
                    
                    } else {
                        res.keys ~= res.transcode (azColName [num].to!string);
                    }
                }
            }

            string [] vals = new string [argc];

            foreach (num; 0..argc) {
                if (argv [num]) {
                    static if (is (String == string)) {
                        vals [num] = argv [num].to!string;
                    } else {
                        vals [num] = res.transcode (argv [num].to!string);
                    }

                } else {
                    vals [num] = "";
                }
            }
            res.rows ~= new LightDbRow (vals, &res.keys); 
            return 0;
        }

        char * errMsg = null; 
        string    cmd = toDbString (command);
        
        int ret = sqlite3_exec (
                        con, 
                        cmd.toStringz, 
                        &callback, 
                        cast (void *) &res, 
                        &errMsg);
        scope (exit) sqlite3_free (errMsg);

        if (ret) {
            string err = errMsg.to!string;
            throw new Exception (err, file, line);
        }

        return res;
    }
}

/** An OutputRange containing lines from a database query. 
    It can be used in a foreach loop: 
    ----
    foreach (dbRow; resultSetInstance) {
        ...
    }
    ----
 */
class SqliteResultSet : ResultSet {
    protected {
        /// The rows in that set.
        DbRow [] rows;
        
        /// Index in the array of rows we are iterating over.
        size_t cursor = 0; 

    }
    
    /// The names of the columns.
    string [] keys;

    /** A delegate to transcode strings from the database.
      This is set by the Connection that instantiate us.
      */
    string delegate (string) transcode; 


    /** Initializes with a value for our transcode delegate member. */
    this (typeof (transcode) dg) {
        transcode = dg;

        if (transcode is null) {
            transcode = (s) => s;
        }
    }

    /// Iterate to the next item.
    void popFront () {
        cursor ++;
    }

    /// Retrieve the current record.
    DbRow front () {
        if (empty) {
            return null;
        }
        return rows [cursor];
    }

    /// True if there is no more records to fetch.
    bool empty () {
        return cursor >= rows.length; 
    }

    /// Returns the length (in number of rows) of this result set.
    ulong length () {
        return rows.length.to!ulong;
    }

    /// Rewind the result set so that it can be iterated once more. 
    bool rewind () {
        cursor = 0;
        return true;
    }
}


version (SqliteMain) {
    void main () {

        auto con = new Sqlite!Latin1String (":memory:");

        con.execute (`
            DROP TABLE IF EXISTS people;
        `);
        con.execute (`
            CREATE TABLE people (
                id  integer,
                name varchar(1024),
                primary key (id)
            );
        `);
        con.execute (`insert into people values (1, 'Ray Jay Ay Emzeeday')`);

        auto rows = con.query (`select * from people`);
        writeln (rows);

        foreach (row; rows) {
            writeln (row ["id"], " =(aa)> ", row ["name"]);
        }


        auto rs = con.select (`select * from people`);

        foreach (row; rs) {
            writeln (row ["id"], " =(rs)> ", row ["name"]);
        }
    }
}

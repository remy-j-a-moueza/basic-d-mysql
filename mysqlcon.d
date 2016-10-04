// vim: inde= cin cino=(0,w4,W0,u2,)0 ft=d
import std.stdio;
import std.string;
import std.conv;
import std.range;
import std.encoding;

import core.stdc.config;
import dbconnection;

/// dmd -L-lmysqlclient <your_module.d> connection.d

/**
  Some mysql definitions. 
*/
alias void  MYSQL; 
alias void  MYSQL_RES; 
alias char** MYSQL_ROW;

struct st_mysql_field
{
	char* name;
	char* org_name;
	char* table;
	char* org_table;
	char* db;
	char* catalog;
	char* def;
	c_ulong length;
	c_ulong max_length;
	uint name_length;
	uint org_name_length;
	uint table_length;
	uint org_table_length;
	uint db_length;
	uint catalog_length;
	uint def_length;
	uint flags;
	uint decimals;
	uint charsetnr;
	enum enum_field_types
	{
		MYSQL_TYPE_DECIMAL = 0,
		MYSQL_TYPE_TINY = 1,
		MYSQL_TYPE_SHORT = 2,
		MYSQL_TYPE_LONG = 3,
		MYSQL_TYPE_FLOAT = 4,
		MYSQL_TYPE_DOUBLE = 5,
		MYSQL_TYPE_NULL = 6,
		MYSQL_TYPE_TIMESTAMP = 7,
		MYSQL_TYPE_LONGLONG = 8,
		MYSQL_TYPE_INT24 = 9,
		MYSQL_TYPE_DATE = 10,
		MYSQL_TYPE_TIME = 11,
		MYSQL_TYPE_DATETIME = 12,
		MYSQL_TYPE_YEAR = 13,
		MYSQL_TYPE_NEWDATE = 14,
		MYSQL_TYPE_VARCHAR = 15,
		MYSQL_TYPE_BIT = 16,
		MYSQL_TYPE_TIMESTAMP2 = 17,
		MYSQL_TYPE_DATETIME2 = 18,
		MYSQL_TYPE_TIME2 = 19,
		MYSQL_TYPE_NEWDECIMAL = 246,
		MYSQL_TYPE_ENUM = 247,
		MYSQL_TYPE_SET = 248,
		MYSQL_TYPE_TINY_BLOB = 249,
		MYSQL_TYPE_MEDIUM_BLOB = 250,
		MYSQL_TYPE_LONG_BLOB = 251,
		MYSQL_TYPE_BLOB = 252,
		MYSQL_TYPE_VAR_STRING = 253,
		MYSQL_TYPE_STRING = 254,
		MYSQL_TYPE_GEOMETRY = 255
	}
	enum_field_types type;
	void* extension;
}

alias st_mysql_field MYSQL_FIELD;
alias ulong my_ulonglong;

extern (C) {
    const(char)* mysql_error (MYSQL * mysql);
    MYSQL * mysql_init (MYSQL * mysql);
    void mysql_close (MYSQL* sock);
    MYSQL* mysql_real_connect (MYSQL* mysql, 
                               const(char)* host,
                               const(char)* user,
                               const(char)* passwd, 
                               const(char)* db, 
                               uint port, 
                               const(char)* unix_socket, 
                               c_ulong clientflag);
    int mysql_query (MYSQL* mysql, const(char)* q);
    MYSQL_RES* mysql_store_result (MYSQL* mysql);
    uint mysql_num_fields (MYSQL_RES* res);
    MYSQL_ROW mysql_fetch_row (MYSQL_RES* result);
    void mysql_free_result (MYSQL_RES* result);
    my_ulonglong mysql_insert_id (MYSQL* mysql);
    MYSQL_FIELD* mysql_fetch_field (MYSQL_RES* result);
    void mysql_data_seek (MYSQL_RES* result, my_ulonglong offset);
    ulong mysql_num_rows (MYSQL_RES* res);
}

/** Throw an exception if the errorCode is non zero. */
int checkError (int errorCode, MYSQL * con, string file, int line) {
    if (errorCode) {
        throw new Exception (mysql_error (con).to!string, file, line);
    }
    
    return errorCode;
}

/** Throw an exception if the given pointer is null. */
void * checkError (void * ptr, MYSQL * con, string file, int line) {
    checkError (ptr is null ? 1 : 0, con, file, line);
    return ptr;
}



/** Minimalist MySQL connection client class.
 */
class MySQL (String = string) : Connection  {

    protected {
        /// The MySQL connectio
        MYSQL * con;
        string _name;
    }

    @property string name () { return _name; }

    /// Initialize an empty connection.
    this (string name = "") {
        this.con  = mysql_init (null);
        this._name = name;

        if (con == null) {
            throw new Exception (mysql_error (con).to!string);
        }
    }

    this (string host, string user, string password, string db, uint port = 0,
          string file = __FILE__, int line = __LINE__) 
    {
        this ();
        connect (host, user, password, db, port, file, line);
    }

    ~this () {
        close ();
    }

    /// Closes the connection.
    void close () {
        mysql_close (con);
    }

    /// Connect to a database.
    Connection connect (string host, string user, string password, string db,
                        uint port = 0,
                        string file = __FILE__, int line = __LINE__) {
        
        mysql_real_connect (con, 
                            host.toStringz, 
                            user.toStringz,
                            password.toStringz,
                            db.toStringz,
                            port,
                            null,
                            0).checkError (con, file, line);
        _name = "%s@%s:%d/%s".format (user, host, port, db);
        return this;
    }

    /// Executes a query, discarding the results.
    void execute (string command, string file = __FILE__, int line = __LINE__) {
        string cmd; 

        static if (is (String == string)) {
            cmd = command;
        } else {
            String tmp;
            .transcode (command, tmp);
            cmd = cast (string) tmp;
        }

        mysql_query (con, cmd.toStringz).checkError (con, file, line);
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
        return mysql_insert_id (con);
    }

    /// Executes a query and retrieve its results.
    string [string][] query (string command, 
                             string file = __FILE__, int line = __LINE__) {
        execute (command, file, line);

        MYSQL_RES * result = mysql_store_result (con).checkError (con, 
                                                                  file, 
                                                                  line);
        int      numFields = mysql_num_fields (result);
        
        // Retrieve the field names.
        MYSQL_FIELD * field;
        string [] fieldNames; 

        foreach (i ; 0 .. numFields) {
            field = mysql_fetch_field (result);
            
            static if (is (String == string)) {
                fieldNames ~= field.name.to!string;
            } else {
                fieldNames ~= transcode (field.name.to!string); 
            }
        }

        // Deal with the rows. 
        MYSQL_ROW      row; 
        string [string][] results;
            
        row = mysql_fetch_row (result);

        while (row !is null) {
            string [string] drow; 
            
            foreach (i; 0 .. numFields) {
                if (row [i]) {
                    static if (is (String == string)) {
                        drow [fieldNames [i]] = row [i].to!string;
                    
                    } else {
                        drow [fieldNames [i]] = transcode (row [i].to!string);
                    }
                } else {
                    row [i] = null;
                }
            }
            results ~= drow;
            row = mysql_fetch_row (result);
        } 
        mysql_free_result (result);

        return results;
    }
    
    
    /** Executes a query and retrieve its results.
      
      The values are fetched as needed, which may improve the performances. 
      The returned ResultSet instance can only be used once. Once the last
      value has been fetched, we cannot rewind the ResultSet.

    */
    ResultSet select (string command, 
                        string file = __FILE__, int line = __LINE__) {
        execute (command, file, line);

        MYSQL_RES * result = mysql_store_result (con).checkError (con, 
                                                                  file, 
                                                                  line);
        int      numFields = mysql_num_fields (result);
        
        // Retrieve the field names.
        MYSQL_FIELD * field;
        string [] fieldNames = new string [numFields]; 

        foreach (i ; 0 .. numFields) {
            field = mysql_fetch_field (result);

            static if (is (String == string)) {
                fieldNames [i] =            field.name.to!string ; 
            } else {
                fieldNames [i] = transcode (field.name.to!string); 
            }
        }

        return new MySqlResultSet (result, fieldNames, &this.transcode);
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
class MySqlResultSet : ResultSet {
    protected {
        /// The native mysql result pointer.
        MYSQL_RES * result;

        /// The current record.
        LightDbRow _front;

        /// True when there is no more record to fetch.
        bool isEmpty;
    }

    /// The names of the columns.
    string [] keys;

    /** A delegate to transcode strings from the database.
      This is set by the Connection that instantiate us.
      */
    string delegate (string) transcode;

    /// Initializes from a low level mysql query.
    this (MYSQL_RES * res, string [] ks, typeof (transcode) dg) {
        result    = res;
        keys      = ks;
        transcode = dg;

        /// transcode needs to be initialized first before calling popFront!!!
        if (transcode is null) {
            transcode = (s) => s;
        }
        popFront ();
    }

    /// Fetch the next record.
    void popFront () {
        MYSQL_ROW row = mysql_fetch_row (result);

        if (row is null) {
            isEmpty = true;
            return;
        }

        string [] vals = new string [keys.length];

        foreach (num, k; keys) {
            vals [num] = row [num] 
                       ? transcode (row [num].to!string)
                       : null;
        }

        if (_front is null) {
            _front = new LightDbRow (vals, &keys);
        
        } else {
            _front.values = vals;
        }
    }

    /// Retrieve the current record.
    DbRow front () {
        return _front;
    }

    /// True if there is no more records to fetch.
    bool empty () {
        return isEmpty;
    }

    /// Returns the length (in number of rows) of this result set.
    ulong length () {
        return mysql_num_rows (result);
    }

    /// Rewind the result set so that it can be iterated once more. 
    bool rewind () {
        mysql_data_seek (result, 0);
        isEmpty = false;
        popFront ();
        return true;
    }

    /// Releases the resources on deletion.
    ~this () {
        mysql_free_result (result);
    }
}



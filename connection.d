// vim: inde= cin cino=(0,w4,W0,u2,)0 ft=d

/+
   A dead simple and basic module to access MySQL databases using the C API. 
   Row values are returned as strings.

   ----
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
    ----

 +/


/* LICENSE (MIT)
 *  
 * Copyright (c) 2015, Rémy J. A. Mouëza. 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Rémy J. A. Mouëza (the author) nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import std.stdio;
import std.string;
import std.conv;

import core.stdc.config;

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

/** Throw an exception if the errorCode is non zero. */
int checkError (int errorCode, Connection con, string file, int line) {
    return checkError (errorCode, con.con, file, line);
}

/** Throw an exception if the given pointer is null. */
void * checkError (void * ptr, MYSQL * con, string file, int line) {
    checkError (ptr is null ? 1 : 0, con, file, line);
    return ptr;
}

/** Throw an exception if the given pointer is null. */
void * checkError (void * ptr, Connection con, string file, int line) {
    checkError (ptr is null ? 1 : 0, con.con, file, line);
    return ptr;
}



/** Minimalist MySQL connection client class.
 */
class Connection {
    static Connection [string] instances;

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

        instances [name] = this;
    }

    ~this () {
        if (name in instances) {
            //writeln ("instances: ", instances);
            //instances.remove (name);
        }
        close ();
    }

    /// Retrieve a connection by name. 
    static Connection get (string name, string file = __FILE__, int line = __LINE__) {
        Connection con = Connection.instances.get (name, null); 

        if (con is null) {
            throw new Exception ("%s (%d): Invalid connection [%s]: null"
                                 .format (file, line, name));
        }
        return con;
    }
    
    /// Closes the connection.
    void close () {
        mysql_close (con);
    }

    /// Connect to a database.
    Connection connect (string host, string user, string password, string db,
                        string file = __FILE__, int line = __LINE__) {
        
        mysql_real_connect (con, 
                            host.toStringz, 
                            user.toStringz,
                            password.toStringz,
                            db.toStringz,
                            0,
                            null,
                            0).checkError (con, file, line);
        return this;
    }

    /// Executes a query, discarding the results.
    void execute (string command, string file = __FILE__, int line = __LINE__) {
        mysql_query (con, command.toStringz).checkError (con, file, line);
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
            fieldNames ~= field.name.to!string; 
        }

        // Deal with the rows. 
        MYSQL_ROW      row; 
        string [string][] results;
            
        row = mysql_fetch_row (result);

        while (row !is null) {
            string [string] drow; 
            
            foreach (i; 0 .. numFields) {
                drow [fieldNames [i]] = row [i] 
                                      ? row [i].to!string
                                      : null ;
            }
            results ~= drow;
            row = mysql_fetch_row (result);
        } 
        mysql_free_result (result);

        return results;
    }
    
    
    /** Executes a query and retrieve its results.
      
      The values are fetched as needed, which may improve the performances. 
      The returned ResultSet can be re-used (iterated over again) using its
      rewind() method that set its back to its first record.
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
            fieldNames [i] = field.name.to!string; 
        }

        return new ResultSet (result, fieldNames);
    }
}

/** A row retrieved from the database */
struct DbRow {
    /// The values as an array of strings.
    string [] values;

    /// A pointer to the column names.
    string []* keys;

    /// Retrieve a value at a given key with optional default..
    string get (string key, string default_) {
        import std.algorithm;
        auto index = countUntil (*keys, key);

        if (index == -1) {
            return default_;
        }

        return values [index];
    }

    /** Retrieve a value at a given key with default set to "".
      ----
      value = dbRowInstance [key];
      ----
    */
    string opIndex (string key) {
        return this.get (key, "");
    }

    /// Return a string representation.
    string toString () {
        string [] kvs;
        
        foreach (num, key; *keys) {
            kvs ~= `"`~ key ~ `": "`~ values [num] ~`"`;
        }
        
        return `[`~ kvs.join (", ") ~ `]`;
    }

    /** Convert that DbRow to an associative array. */
    string [string] asAA () {
        string [string] aa;
        
        foreach (num, key; *keys) {
            aa [key] = values [num];
        }
        
        return aa;
    }
    
    /** Like an Associative Array "in" operator:  
      ----
      if ("key" in dbRowInstance) {...}
      ----
     Return a pointer to the column name if one is found matching the given key, 
     otherwise return a null pointer. 
     */
    string * opBinaryRight (string op:"in", T : string) (T key) {
        auto index = countUntil (*keys, key);

        if (index == -1) {
            return null;
        }

        return & (*keys) [index];
    }

    /** Iterating over this.values. 
     ----
     foreach (value; dbRowInstance) {
        ...
     }
     ----
     */
    int opApply (int delegate (ref string val) dg) {
        int result = 0; 

        foreach (num, key; *keys) {
            result = dg (values [num]);

            if (result) 
                break;
        }
        return result;
    }
    
    /** Iterating over the keys and values. 
     ----
     foreach (key, value; dbRowInstance) {
        ...
     }
     ---
     */
    int opApply (int delegate (ref string key, ref string val) dg) {
        int result = 0; 

        foreach (num, key; *keys) {
            result = dg (key, values [num]);

            if (result) 
                break;
        }
        return result;
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
class ResultSet {
    protected {
        /// The native mysql result pointer.
        MYSQL_RES * result;

        /// The current record.
        DbRow _front;

        /// True when there is no more record to fetch.
        bool isEmpty;
    }

    /// The names of the columns.
    string [] keys;

    /// Initializes from a low level mysql query.
    this (MYSQL_RES * res, string [] ks) {
        result = res;
        keys   = ks;
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
                       ? row [num].to!string
                       : null;
        }

        _front = DbRow (vals, &keys);
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
    void rewind () {
        mysql_data_seek (result, 0);
        isEmpty = false;
        popFront ();
    }

    /// Releases the resources on deletion.
    ~this () {
        mysql_free_result (result);
    }
}



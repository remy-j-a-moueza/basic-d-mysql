// vim: inde= cin cino=(0,w4,W0,u2,)0 ft=d
import std.stdio;
import std.string;
import std.algorithm;
import std.array;
import std.conv;
import std.range;
import std.regex;
import std.string;
import std.encoding;



/** An interface that represents a row from the database.
*/
interface DbRow {
    /// Get a key; if no values exist for it, return the given default.
    string get (string key, string default_);

    /** Retrieve a value at a given key with default set to "".
      ----
      value = dbRowInstance [key];
      ----
    */
    string opIndex (string key);


    /** Like an Associative Array "in" operator:  
      ----
      if ("key" in dbRowInstance) {...}
      ----
     Return a pointer to the column name if one is found matching the given key, 
     otherwise return a null pointer. 
     */
    string * opBinaryRight (string op:"in", T : string) (T key);
    
    /** Iterating over this.values. 
     ----
     foreach (value; dbRowInstance) {
        ...
     }
     ----
     */
    int opApply (int delegate (ref string val) dg);


    /** Iterating over the keys and values. 
     ----
     foreach (key, value; dbRowInstance) {
        ...
     }
     ---
     */
    int opApply (int delegate (ref string key, ref string val) dg);
}


/** Represents a set of records (DbRow instances) retrieved from the database. */
interface ResultSet {

    /// Iterates to the next record.
    void popFront ();
    
    /// Retrieve the current record
    DbRow front ();

    /// True if there is no more records to fetch.
    bool empty ();

    /// Returns the length (in number of rows) of this result set.
    ulong length ();

    /* Rewind the result set so that it can be iterated once more. 

       Returns true if we succesfully rewinded the ResultSet, false if we could
       not or if this operation is not supported by the implementation.
    */
    bool rewind ();
}


/** Represents a connection to a database.  */
interface Connection {

    /** Executes a query, discarding the results.

      The file and line arguments are used to identified the caller's site when
      we raise an exception.
     */
    void execute (string command, 
                  string file = __FILE__, int line = __LINE__);

    /** Executes a query, returning the results as a convenient associative
      array.

      The file and line arguments are used to identified the caller's site when
      we raise an exception.
     */
    string [string][] query (string command,
                             string file = __FILE__, int line = __LINE__);
    
    /** Executes a query, returning the results as a ResultSet instance.
      The ResultSet/DbRow couple may allowed for better performance depending
      on the database we use.

      The file and line arguments are used to identified the caller's site when
      we raise an exception.
     */
    ResultSet select (string command, 
                      string file = __FILE__, int line = __LINE__);
    
    /// Retrieve the last inserted ID. 
    ulong lastInsertId (); 

    /// Retrieve a string representation to identify this connection.
    string name ();
}

/** A thin wrapper around an associative array to implement the DbRow interface.
 */
class BasicDbRow : DbRow {
    protected {
        string [string] row;
    }

    this (string [string] aa) {
        this.row = aa;
    }

    override string toString () {
        return "%s".format (row);
    }

    string get (string key, string default_) {
        return row.get (key, default_);
    }

    string opIndex (string key) {
        return this.get (key, "");
    }

    string * opBinaryRight (string op:"in", T : string) (T key) {
        return key in row;
    }

    int opApply (int delegate (ref string val) dg) {
        int result = 0; 

        foreach (num, key; row.keys) {
            result = dg (row [key]);

            if (result) 
                break;
        }
        return result;
    }
    
    int opApply (int delegate (ref string key, ref string val) dg) {
        int result = 0; 

        foreach (key, ref val; row) {
            result = dg (key, val);

            if (result) 
                break;
        }
        return result;
    }

}


/** A row retrieved from the database.

 We hold an array of values and a pointer of an array of keys.
 This way we don't allocate memory the keys several time as is done with a 
 string [string][] in Connection.query().
*/
class LightDbRow : DbRow {
    /// The values as an array of strings.
    string [] values;

    /// A pointer to the column names.
    string []* keys;

    this (string [] vals, string []* ks) {
        values = vals;
        keys   = ks;
    }

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
    override string toString () {
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

/** A thin wrapper around an input range object to implement the ResultSet
   interface.
    
    WARNING: the rewind() method has no effects.
*/
class BasicResultSet : ResultSet {

    InputRange!DbRow range; 
    
    this (InputRange!DbRow rg) {
        range = rg;
    }
    
    void popFront () {
        range.popFront ();
    }

    DbRow front () {
        return range.front;
    }

    bool empty () {
        return range.empty;
    }

    ulong length () {
        if (empty)
            return 0;
        return 1;
    }

    /// WARNING: this method has no effects.
    bool rewind () {
        return false;
    }
}


/** A template predicate for Row types either:
    - an associative array, with key and values as strings.
    - a DbRow struct that work similarily to the associative array.
  */
enum isRowType (T) = is (T : string [string]) || is (T : DbRow);

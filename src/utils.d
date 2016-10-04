// vim: inde= cin cino=(0,w4,W0,u2,)0 ft=d
import std.stdio;
import std.string;
import std.algorithm;
import std.array;
import std.conv;
import std.range;
import std.regex;
import std.string;

/** Repeat the given string "num" times. */
string repeat (string str, int num) {
    string ss; 
    
    foreach (n; 0 .. num) {
        ss ~= str;
    }
    return ss;
}

/** Escape the given string for json. */
string escJson (string str) {
    char [] esc;

    auto cmap = [
        '"'  : `\"`,
        '\\' : `\\`,
        '\b' : `\b`,
        '\f' : `\f`,
        '\n' : `\n`,
        '\r' : `\r`,
        '\t' : `\t`,
    ];

    foreach (ch; str) {
        if (auto ptr = ch in cmap) {
            esc ~= *ptr;
        
        } else {
            esc ~= ch;
        }
    }
    return esc.idup;
}

/* Basic sql escaping. */
string escSql (string val) {
    return "'" ~ val.replace ("'", "''")
                    .replace (`\`, `\\`)
               ~ "'";
}

string escHtml (string val) {
    static escapes = [
        ["<", "&lt;"],
        [">", "&gt;"]
    ];
    
    string res = val;

    foreach (pair; escapes) {
        res = res.replace (pair [0], pair [1]);
    }

    return res;
}

string stripTags (string val) {
    import std.regex;
    static tags = ctRegex!(`<[^>]*>`);
    return val.replaceAll (tags, "");
}

/** Return a md5 hash of the given string. */
string md5 (string ss) {
    import std.digest.md : md5Of;
    import std.base64;

    return Base64.encode (md5Of (ss));
}

/** C++ like console stream: 
  ----
  Console cout;
  cout << "hello world!" << endl;
  ----
*/
struct Console {
    Console opBinary (string op : "<<", T) (T val) {
        write (val);
        return this;
    }
}

/// C++ like output "stream" to the console.
Console cout;

/// End of line separator (\n).
immutable endl = "\n";

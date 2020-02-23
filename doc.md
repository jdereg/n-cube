This method returns a `Closure` to be used with the `NCube.mapReduce()` API for `NCubes` that follow the 'Decision Table' pattern.  The 'Decision Table' pattern is used for a concise representation of rules against multiple variables.  They are used by taking a set of inputs (say a product selection by a user) and then passing attributes about that product selection to the decision table to get a price.  It could also be used to get a commission rate or insurance rate.

Decision ables have **inputs** and **outputs**. The inputs are decision variables that are either discrete values [meaning they must match] or range values [meaning the input attributes must be `>=` to the low values in the range, and `<` the high value of the range.].  There are also a few other special column types.

For example, a situation where 3 key decision variables (sku, region, campaign) and a `Date` Range select a product price. In addition to the decision input variables, there are also one or more outcomes (outputs).  Below are the supported column types:
 * A **decision variable**.  Column must have meta-property attribute `input_value`.
 * A **range decision** variable.  Two columns are used to specify the lower bound and upper bound.  The column defining the lower bound values must have the `input_low` meta-property, while the column defining the upper bound has the meta-property `input_high`.  The value for both of these meta-properties (`input_low` and`input_high`) is the name of the key that will be on input to be tested for inclusion.
 * An **output**.  Column must have the meta-property attribute `output_value`.
 * Disable column. A column with `ignore` meta-property.
 * No meta-properties.  Columns that exist to provide descriptions, etc. to associated columns.  These are not processed.
 
 It is usually expected that these tables return 0 or 1 row for a given set of inputs.

There are two additional special columns that are allowed.  A column with the 'ignore' meta-property allows you to put true in the cell value under this column and that row will not be selected.  This allows you to start building out your table, with new rows, but not 'turning them on' until later (removing the 'true' value from the cell residing within the ignore column). Finally, you can set the meta-property 'priority' on a Column.  Typically this is set as a value in a cell to help eliminate duplicate outputs.  For example, you may have a wide date range and a base price.  Then you have rows with narrower banded date ranges that have a differnt price.  If two rows are being returned, you fix this by setting the priority to values that are different for each row.  The row with the highest priority (lowest value - 1 is the highest priorty) will be returned.  Finally, any columns that are added that have no meta-properties on them are allowed, such as description columns which makes it easier for those editing tables with special codes in them.

In order to identify the data-type of the input(decision variable) columns, these columns should have a
'data_type' meta property.  This indicates the data type the values in this column will be coerced
when compared to the input variable associated to this column.  If the 'data_type' meta property is not
specified, then the data_type is defaulted to 'STRING'.  The data types supported are:
```
CISTRING	// For case insensitive Java Strings.  Strings will be compared with .equalsIgnoreCase()
STRING      // For Java Strings.  Strings will be compared with .equals()
LONG        // For any integral java type (byte, short, int, long).  All of those will be promoted to long internally.
BIG_DECIMAL // For float, double, or BigDecimal.  All of those will be promoted to BigDecimal internally.
DOUBLE      // For float or double.  Float will be promoted to double internally.
DATE        // For Date.  Calendar and Long can be passed in for comparison against.
COMPARABLE	// For all other objects.  For example, Character, LatLon, or a Class that implements Comparable.
```
In order to use this closure with mapReduce(), you must supply input to map reduce in the following format:
```
Map additionalInput = [
    dvs: [
        profitCenter: '1234',                            // regular decision variable
        producerCode: '50',                              // regular decision variable
        symbol: 'FOO'                                    // regular decision variable
        ignore: null,                                    // need to include the 'ignore' column
        date: [                                          // Name of a range variable (right hand side of 'input_low' and 'input_high' on range colums)
            low:'effectiveDate',                         // Name of column with low range
            high:'expirationDate',                       // Name of column with high end of range
            value: new Date()                            // Value that will be tested for inclusion: low >= value < high
        ]
    ]
]

Map options = [
        (NCube.MAP_REDUCE_COLUMNS_TO_SEARCH): inputColumns,  // List of column names (decision vars)
        (NCube.MAP_REDUCE_COLUMNS_TO_RETURN): outputColumns, // List of column names (output values)
        input: additionalInput                               // see Structure above
]

long start = System.nanoTime()
Map result = ncube.mapReduce('field', find, options)
```
@return Closure
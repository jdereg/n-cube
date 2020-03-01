# Decision Table
The `DecisionTable` class is instantiated with a 2D `NCube`, where one axis contains
decision columns and output columns, denoted by meta-properties, and the other axis
represents rows.  

The main API is `getDecision(Map<String, ?> input)` which scans the table using the input coordinate, 
and returns the rows that match it.  The `Map<> input` contains the keys that match columns with
`INPUT_VALUE: true` on their meta-properties.  The key value has the same text as the column value
on the 2D `NCube` 'field' axis.  The value side of the input coordinate is compared with equality
against all rows in the NCube.

If there is more than one column marked with `INPUT_VALUE true`, then each of those columns must
match a value in the same row in order for the row to be returned. Actually, the row is not returned
but all the values in the columns that are marked with the `OUTPUT_VALUE: true` are returned.

The return is a `Map` looks like:
```groovy
['134': [price: 1.0, commission: 15.0]]
```                                     
where `134` is the row ID (the row axis can have any name, must be `Axis.axisType (DISCRETE)`, and can be any 
`Axis.axisValueType (LONG, STRING, etc.)`.

In this example, the input was:
```groovy
[state:'OH', 'SKU': 12345]
```
and that input matched row `134` which had a value of `OH` for `state` and the value `12345` for `SKU`.
Both the `state` column and the `SKU` column have the meta-property `INPUT_VALUE: true`.

###Field axis
The field axis contains the top columns of the decision table.  The axis can have any name you want.
The field axis must be a `DISCRETE` axis, with the value type of `CISTRING`.

On the field axis, some columns are meant to match the input `Map`, some columns contain the output values.
Here are all the special meta-properties and their meanings:
  
* `INPUT_VALUE` with value `true`: This is a decision variable, and the column name is matched against the input coordinate key. 
* `OUTPUT_VALUE` with value `true`: This is an output variable. The values in these columns are returned from the matched rows.
* `INPUT_LOW` with the value that matches the name of a key that will be supplied for a range input.  For example, `date`
* `INPUT_HIGH` with the value that matches the name of a key that will be supplied for a range input.  For example, `date`
  * The `INPUT_LOW` and `INPUT_HIGH` columns work to match range inputs, like dates, etc. In this example, if the input
    `Map` contains a key of `date: 2020/02/14` then it will match the range if the input date is >= (greater than or equal) to
    the row value in the 'low' column and < (less than) the value in the 'high' column.
* `DATA_TYPE` must be specified on range columns (ones marked with `INPUT_LOW` and `INPUT_HIGH`).  This indicates the
data type that will be compared against the ranges. Valid values are `DATE`, `LONG`, `BIG_DECIMAL`, and `DOUBLE`.

On the field axis, if you name a column `Ignore` (case does not matter), then any row will be ignored (skipped) if the 
cell in the `Ignore` column evaluates to boolean `true`.  This column is optional.

On the field axis, if you name a column `Priority` (case does not matter), any two or more rows match, only the rows
with the highest matching priority will be returned.  `1` is considered the highest priority.  This column is optional.

###Row axis
The row axis typically has values 1 through the number of rows.  This axis must be `DISCRETE`, however, the axis value
type can be `LONG`, `STRING`, `CISTRING`, `DOUBLE`, `EXPRESSION`, `BIG_DECIMAL`, or `COMPARABLE`.  The values from
the `Columns` on the row axis are not matched against any input, however, they are used as the keys in the `Map` of 
the returned matching rows. 
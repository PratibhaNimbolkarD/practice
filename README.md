## RDD

### **count():**
**Action**: Returns the number of elements in the RDD.

### first():
**Action:** Returns the first element of the RDD.

### take(n):
**Action:** Returns an array with the first 'n' elements of the RDD.

### foreach(func):
**Action:** Applies the specified function 'func' to each element of the RDD. Note: This action is primarily used for performing side effects and does not return a value.

### min():
**Action:** Returns the minimum element in the RDD. The elements should be comparable.

### max():
**Action:** Returns the maximum element in the RDD. The elements should be comparable.

### mean():
**Action:** Returns the mean (average) value of the elements in the RDD. The elements should be numeric.

### reduce(func):
**Transformation:** Aggregate the elements of the RDD using a specified associative and commutative function 'func'.

### filter(func):
**Transformation:** Returns a new RDD containing only the elements that satisfy the specified predicate function 'func'.

### mapPartitions(func):
**Transformation:** Applies a function 'func' to each partition of the RDD.

### mapPartitionsWithIndex(func):
**Transformation:** Similar to mapPartitions(), but also provides the index of each partition.

### flatMap(func):
**Transformation:** Similar to map(), but each input item can be mapped to 0 or more output items. The output is flattened into a single RDD.


## Data_frame

* Creating a DataFrame involves organizing distributed data into named columns.
* Schemas define the structure of DataFrames, including column names and data types.
* Adding a column enriches or transforms data by introducing new attributes.
* Renaming a column improves clarity and consistency in column naming conventions.
* Reading CSV files into DataFrames facilitates data exploration and analysis.
* Joining combines data from multiple DataFrames based on common columns or keys.
* Inner Join retains only rows with matching values in both DataFrames.
* Full Join retains all rows from both DataFrames, filling in missing values with nulls.
* Left Join retains all rows from the left DataFrame, filling in missing values with nulls for the right DataFrame.
* Right Join retains all rows from the right DataFrame, filling in missing values with nulls for the left DataFrame.
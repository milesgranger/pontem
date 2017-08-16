# pontem
Treat PySpark DataFrames like pandas.  

_This is currently just a hobby project, not suitable for use._
---

Turn somethinig like this:  
```python
# Pure PySpark API; df is type pyspark.sql.DataFrame
def multiply(n):
    return udf(lambda col: col * n, FloatType())
df = df.withColumn('new_col', df.select(multiply(2)(df['other_col'])))
```  

...into this:  
```python
# Using pontem.core.DataFrame object.
df['new_col'] = df['other_col'] * 2
```



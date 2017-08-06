# pontem
Treat PySpark DataFrame like pandas.  

_This is currently just a hobby project, not suitable for use._
---

Turn somethinig like this:  
```python
df = df.withColumn('new_col', udf(lambda col: col * 2), df['other_col'])
```  

...into this:  
```python
df['new_col'] = df['other_col'] * 2
```



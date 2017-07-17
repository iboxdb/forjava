### DEMOS

##### KeyOnly.java Output     [Link](https://github.com/iboxdb/forjava/blob/master/demos/KeyOnly.java)

```
Insert TotalObjects: 1,000,000 FileSize: 158MB
Elapsed 10.314s, AVG 96,955 o/sec

Select TotalObjects: 2,000,000
Elapsed 2.493s, AVG 802,246 o/sec

Select TotalObjects: 2,000,000 -Reopen
Elapsed 2.725s, AVG 733,944 o/sec
```


**KeyOnly table doesn't have 'Value',  not support update the 'Value', use 'delete() and insert()' to update KeyOnly table**

````
void update(Box box, Object oldKey, object newKey){
   box.d("/N", oldKey).delete();
   box.d("/N").insert(newKey);
}
box.commit();
````

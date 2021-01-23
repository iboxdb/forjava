### DEMOS

##### KeyOnly.java Output     [Link](https://github.com/iboxdb/forjava/blob/master/demos/KeyOnly.java)

```
Insert TotalObjects: 1,000,000 FileSize: 161MB
Elapsed 14.981s, AVG 66,751 o/sec

Select TotalObjects: 2,000,000
Elapsed 1.312s, AVG 1,524,390 o/sec

Select TotalObjects: 2,000,000 -Reopen
Elapsed 2.822s, AVG 708,717 o/sec

Select TotalObjects: 2,000,000 -Reopen2
Elapsed 0.514s, AVG 3,891,050 o/sec
```


**KeyOnly table doesn't have 'Value',  not support update the 'Value', use 'delete() and insert()' to update KeyOnly table**

````
void update(Box box, Object oldKey, object newKey){
   box.d("/N", oldKey).delete();
   box.d("/N").insert(newKey);
}
box.commit();
````

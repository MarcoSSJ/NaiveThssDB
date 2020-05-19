package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row>, Serializable {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  public int primaryIndex;

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    Column column = new Column("c1", ColumnType.INT, 1, false, 100);
    Column[] columns = {column};
    Table table = new Table("testdb", "testtable", columns);
    Entry entry = new Entry("data");
    table.delete(entry);
    table.recover();
    table.delete(entry);
    /*Entry entry = new Entry("data");
    Entry[] entries = {entry};
    Row row = new Row(entries);
    table.insert(row);
    System.out.println(table);
    table.serialize();
    table.deserialize();
    table.delete(entry);
    String name = "./data/" + table.databaseName + '/' + table.tableName + "0";
    FileInputStream fileIn = new FileInputStream(name);
    ObjectInputStream in = new ObjectInputStream(fileIn);
    Table t1 = (Table) in.readObject();
    t1.delete(entry);
    t1.delete(entry);
    System.out.println(t1.tableName);
    in.close();
    fileIn.close();*/
  }

  public Table(String databaseName, String tableName, Column[] columns) throws IOException, ClassNotFoundException {
    // TODO
    if(columns!=null) {
      this.databaseName = databaseName;
      this.tableName = tableName;
      this.columns = new ArrayList<>(Arrays.asList(columns));
      this.index = new BPlusTree<>();
      primaryIndex = -1;
      for (int i = 0; i < columns.length; i++) {
        if (columns[i].isPrimary() == 1) {
          primaryIndex = i;
          break;
        }
      }
      if (primaryIndex == -1) {
        //no primary key exception
      }
    }
    else{
      this.databaseName = databaseName;
      this.tableName = tableName;
      recover();
    }
  }

  private void recover() throws IOException, ClassNotFoundException {
    // TODO
    try {
      this.deserialize();
    }catch (FileNotFoundException e)
    {
      //
    }
  }

  public void insert(Row row) {
    // TODO
    Entry entry = row.getEntries().get(primaryIndex);
    try{
      index.get(entry);
      //exception

    }
    catch(KeyNotExistException e){
      index.put(entry, row);
    }
  }

  public void delete(Entry entry) {
    // TODO
    if(entry==null){
      // exception
    }
    try{
      index.remove(entry);
      System.out.println("delete");
    }
    catch (KeyNotExistException e){
      System.out.println("key not exist");
    }
  }

  public void delete(String comparator, String attrName, String attrValue){
    int attrIndex = this.getIndex(attrName);
    TableIterator tableIterator = new TableIterator(this);
    Entry attrValueEntry = new Entry(attrValue);
    switch (comparator)
    {
      case "=":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          //System.out.println(currentRow);
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)==0){
            index.remove(currentRow.getEntries().get(primaryIndex));
          }
        }
        break;
      case "<":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)<0){
            index.remove(currentRow.getEntries().get(primaryIndex));
          }
        }
        break;
      case ">":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)>0){
            index.remove(currentRow.getEntries().get(primaryIndex));
          }
        }
        break;
      case "<=":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)<=0){
            index.remove(currentRow.getEntries().get(primaryIndex));
          }
        }
        break;
      case ">=":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)>=0){
            index.remove(currentRow.getEntries().get(primaryIndex));
          }
        }
        break;
      case "<>":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)!=0){
            index.remove(currentRow.getEntries().get(primaryIndex));
          }
        }
        break;
      default:
        break;
    }
    return;
  }

  public Row getRow(Entry entry){return index.get(entry);}


  public void update(String attrToBeUpdate, String valueToBeUpdate, String comparator, String attrName, String attrValue) {
    // TODO
    int attrToBeUpdateIndex = this.getIndex(attrToBeUpdate);
    int attrIndex = this.getIndex(attrName);
    TableIterator tableIterator = new TableIterator(this);
    Entry attrValueEntry = new Entry(attrValue);
    Entry valueToBeUpdateEntry = new Entry(valueToBeUpdate);
    switch (comparator)
    {
      case "=":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)==0){
            currentRow.update(attrToBeUpdateIndex, valueToBeUpdateEntry);
          }
        }
        break;
      case "<":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)<0){
            currentRow.update(attrToBeUpdateIndex, valueToBeUpdateEntry);
          }
        }
        break;
      case ">":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)>0){
            currentRow.update(attrToBeUpdateIndex, valueToBeUpdateEntry);
          }
        }
        break;
      case "<=":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)<=0){
            currentRow.update(attrToBeUpdateIndex, valueToBeUpdateEntry);
          }
        }
        break;
      case ">=":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)>=0){
            currentRow.update(attrToBeUpdateIndex, valueToBeUpdateEntry);
          }
        }
        break;
      case "<>":
        while(tableIterator.hasNext()){
          Row currentRow = tableIterator.next();
          if(currentRow.getEntries().get(attrIndex).compareTo(attrValueEntry)!=0){
            currentRow.update(attrToBeUpdateIndex, valueToBeUpdateEntry);
          }
        }
        break;
      default:
        break;
    }
    return;
  }

  public void select(ArrayList<ArrayList> conditions){
    ArrayList arrayList = new ArrayList();
    ArrayList rows = new ArrayList();
    TableIterator tableIterator = new TableIterator(this);
    if(conditions==null)
    {
      if(tableIterator.hasNext()){
        Row row = tableIterator.next();
        rows.add(row);
      }
      //return rows;
    }
    else{
      ArrayList attrNames = conditions.get(0);
      ArrayList condition = conditions.get(1);
      String compareType = condition.get(0).toString();
      String argv1 = condition.get(1).toString();
      String argv2 = condition.get(2).toString();
      int argvIndex1 = getIndex(argv1);
      int argvIndex2 = getIndex(argv2);
      while(tableIterator.hasNext()){
        Row row = tableIterator.next();
        ArrayList<Entry> entries = row.getEntries();
        Entry entry1 = entries.get(argvIndex1);
        Entry entry2 = entries.get(argvIndex2);
        if(compareType.compareTo("==")==0){
          if(entry1.compareTo(entry2)==0){
            rows.add(row);
          }
          else{
            continue;
          }
        }
        else if(compareType.compareTo("<")==0) {
          if (entry1.compareTo(entry2) < 0) {
            rows.add(row);
          } else {
            continue;
          }
        }
        else if(compareType.compareTo("<=")==0) {
          if (entry1.compareTo(entry2) < 0 || entry1.compareTo(entry2)==0) {
            rows.add(row);
          } else {
            continue;
          }
        }
        else if(compareType.compareTo(">")==0){
          if(entry1.compareTo(entry2) > 0){
            rows.add(row);
          } else {
            continue;
          }
        }
        else if(compareType.compareTo(">=")==0){
          if(entry1.compareTo(entry2) > 0||entry1.compareTo(entry2)==0){
            rows.add(row);
          } else {
            continue;
          }
        }
      }
    }
  }

  private void serialize() throws IOException {
    // TODO
    /*String name = "./data/" + databaseName + '/' + tableName + "0";
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(name));
    oos.writeObject(this);
    oos.close();*/
    String path = "./data/" + databaseName + '/' + tableName;
    File table = new File(path);
    if(table.exists()){
      //exception
    }else{
      table.mkdir();
    }

    String name = path + "/bplustree";
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(name));
    oos.writeObject(this.index);
    oos.close();
    ArrayList info = new ArrayList();
    info.add(this.databaseName);
    info.add(this.tableName);
    info.add(this.columns);
    info.add(this.primaryIndex);
    name = path + "/info";
    oos = new ObjectOutputStream(new FileOutputStream(name));
    oos.writeObject(info);
    oos.close();
  }

  private ArrayList<Row> deserialize() throws IOException, ClassNotFoundException {
    // TODO
    String path = "./data/" + this.databaseName + '/' + this.tableName;
    FileInputStream fileIn = new FileInputStream(path+"/info");
    ObjectInputStream in = new ObjectInputStream(fileIn);
    ArrayList info = (ArrayList)in.readObject();
    this.columns = (ArrayList)info.get(2);
    this.primaryIndex = (int)info.get(3);

    fileIn = new FileInputStream(path+"/bplustree");
    in = new ObjectInputStream(fileIn);
    this.index = (BPlusTree<Entry, Row>) in.readObject();
    return null;
  }

  private int getIndex(String name){
    int size = columns.size();
    for(int i = 0; i < size; i++){
      if(columns.get(i).compareTo(name)==0){
        return i;
      }
    }
    //exception
    return -1;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().getValue();
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}

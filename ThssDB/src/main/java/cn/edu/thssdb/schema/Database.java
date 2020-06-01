package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.type.ColumnType;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    Database db = new Database("testdb");
    Column column = new Column("c1", ColumnType.INT, 1, false, 100);
    Column[] columns = {column};
    Entry entry = new Entry("data");
    Entry[] entries = {entry};
    Row row = new Row(entries);
    db.create("testtable", columns);
    db.persist();
  }

  public Database(String name) throws IOException, ClassNotFoundException {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    String path = "./data/"+this.name;
    File db = new File(path);
    if(db.exists()){
      //
      recover();
    }else{
      db.mkdir();
    }
  }

  private void persist() throws IOException
  {
    // TODO
    String path = "./data/"+this.name;
    File db = new File(path);
    if(db.exists()){
      //
    }else{
      db.mkdir();
    }
    ArrayList tableList = new ArrayList();
    Iterator iterator = tables.entrySet().iterator();
    while (iterator.hasNext()){
      HashMap.Entry entry = (HashMap.Entry)iterator.next();
      String name = (String)entry.getKey();
      tableList.add(name);
    }
    String name = path + "/tableList";
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(name));
    oos.writeObject(tableList);
    oos.close();
  }

  public void write() throws IOException {
    persist();
  }


  public void create(String name, Column[] columns) throws IOException, ClassNotFoundException {
    // TODO
    if(tables.get(name)!=null){
      //exception
    }
    Table table = new Table(this.name, name, columns);
    tables.put(name, table);
    //persist();
  }

  public Table getTable(String name){
    return tables.get(name);
  }

  public void drop(String name) throws IOException {
    // TODO
    if(tables.get(name)==null){
      //exception
    }
    tables.remove(name);
    //persist();
  }

  public String select(QueryTable[] queryTables) throws IOException, ClassNotFoundException {
    // TODO
    //QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() throws IOException, ClassNotFoundException {
    // TODO
    String path = "./data/"+this.name;
    File db = new File(path);
    if(db.exists()){
      //
      try {
        FileInputStream fileIn = new FileInputStream(path + "/tableList");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        ArrayList<String> tableList = (ArrayList) in.readObject();
        in.close();
        System.out.println(tableList);
        for(String name:tableList)
        {
          Table table = new Table(this.name, name, null);
          tables.put(name, table);
        }
      }catch (FileNotFoundException e){
        //
      }
    }
  }

  public void quit() throws IOException {
    // TODO
    //persist();
  }
}

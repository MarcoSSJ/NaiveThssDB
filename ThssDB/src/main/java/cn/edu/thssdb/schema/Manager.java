package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  public Database database;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    Manager manager = new Manager();
    manager.createDatabaseIfNotExists("testdb");
    manager.deleteDatabase("testdb");
  }

  public Manager() {
    // TODO
    databases = new HashMap<String, Database>();
  }

  private void persist() throws IOException
  {
    // TODO
    String path = "./data";
    File db = new File(path);
    if(db.exists()){
      //
    }else{
      db.mkdir();
    }
    ArrayList dbList = new ArrayList();
    Iterator iterator = databases.entrySet().iterator();
    while (iterator.hasNext()){
      HashMap.Entry entry = (HashMap.Entry)iterator.next();
      String name = (String)entry.getKey();
      dbList.add(name);
    }
    String name = path + "/dbList";
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(name));
    oos.writeObject(dbList);
    oos.close();
  }

  public void recover()
  {
    try {
      FileInputStream fileIn = new FileInputStream("./data/dbList");
      ObjectInputStream in = new ObjectInputStream(fileIn);
      ArrayList<String> dbList = (ArrayList) in.readObject();
      in.close();
      for(String name:dbList)
      {
        databases.put(name, null);
      }
    }catch (FileNotFoundException e){
      //
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void use(String name)
  {
    try{
      if(databases.get(name)==null){
        Database db = new Database(name);
        databases.put(name, db);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  private void createDatabaseIfNotExists(String name) throws IOException, ClassNotFoundException {
    // TODO
    Database database = new Database(name);
    databases.put(name, database);
    File db = new File("./data/"+name);
    if(db.exists()){
      //exception
    }else{
      db.mkdir();
    }
  }

  public void deleteDatabase(String name) {
    // TODO
    databases.remove(name);
    File db = new File("./data/"+name);
    if(db.isFile()&&db.exists()){
      db.delete();
    }else{
      //exception
    }
  }

  public void switchDatabase(String name) {
    // TODO
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}

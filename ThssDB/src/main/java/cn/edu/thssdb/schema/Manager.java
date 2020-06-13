package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  //public Database database;
  private HashMap<Long, String> users;
  private HashMap<Long, Boolean> transaction;
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
    databases = new HashMap<>();
    users = new HashMap<>();
    transaction = new HashMap<>();
    recover();
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
        Database database = new Database(name);
        databases.put(name, database);
      }
      System.out.println(databases.toString());
    }catch (FileNotFoundException e){
      //
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void use(long sessionID, String name)
  {
    System.out.println("use"+name);
    if(databases.get(name)!=null){
      //Database db = new Database(name);
      //databases.put(name, db);
      users.put(sessionID, name);
    }
    else{
      //TODO: database not found exception

    }
  }

  public void createDatabaseIfNotExists(String name) throws IOException, ClassNotFoundException {
    // TODO
    Database database = new Database(name);
    databases.put(name, database);
    File db = new File("./data/"+name);
    if(db.exists()){
      //exception
    }else{
      db.mkdir();
    }
    persist();
  }

  public void deleteDatabase(String name) throws IOException{
    // TODO
    databases.remove(name);
    File db = new File("./data/"+name);
    if(db.isFile()&&db.exists()){
      db.delete();
    }else{
      //exception
    }
    persist();
  }

  public void switchDatabase(String name) {
    // TODO
  }

  public Database getDatabase(Long sessionID){
    String name = users.get(sessionID);
    return databases.get(name);
  }

  public void write() throws IOException {
    persist();
  }

  public void beginTransaction(Long sessionID){
    transaction.put(sessionID, Boolean.TRUE);
  }

  public Boolean isTransaction(Long sessionID){
    if(transaction.get(sessionID)==null)
      return Boolean.FALSE;
    return transaction.get(sessionID);
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}

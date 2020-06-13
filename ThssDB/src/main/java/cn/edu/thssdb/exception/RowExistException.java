package cn.edu.thssdb.exception;

public class RowExistException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: row already exist!";
    }
}

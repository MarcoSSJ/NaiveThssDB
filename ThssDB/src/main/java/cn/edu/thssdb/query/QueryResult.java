package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cn.edu.thssdb.schema.Table;
import javafx.scene.control.Cell;

public class QueryResult {

	private List<MetaInfo> metaInfoInfos;
	private List<Integer> index;
	private List<Cell> attrs;
	public Table newTable;
	private QueryTable queryTable;
	private ArrayList<String> result;

	public QueryResult(QueryTable queryTable, ArrayList<String> columns)
	{
		this.queryTable = queryTable;
		this.result = new ArrayList<>();
		index = new ArrayList<>();
		for(int i = 0; i < columns.size(); i++){
			for(int j = 0; j < this.queryTable.columns.size(); j++){
				if(this.queryTable.columns.get(j).compareTo(columns.get(i))==0){
					index.add(j);
					break;
				}
			}
		}
		Iterator iterator = queryTable.rows.iterator();
		while (iterator.hasNext()){
			Row row = (Row)iterator.next();
			ArrayList<String> resRow = new ArrayList<>();
			for(int i = 0; i < index.size(); i++)
			{
				resRow.add(row.getEntries().get(index.get(i)).toString());
			}
			result.add(resRow.toString());
		}
	}

	public Row generateQueryRecord(Row row) {
		// TODO
		return null;
	}

	public ArrayList<String> result(){
		ArrayList<String> res = new ArrayList<>();
		Iterator iterator = this.result.iterator();
		while (iterator.hasNext()){
			String row = iterator.next().toString();
			row = row.substring(1, row.length()-1);
			res.add(row);
		}
		return res;
	}
}
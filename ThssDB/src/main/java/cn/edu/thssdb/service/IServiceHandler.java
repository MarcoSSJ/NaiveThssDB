package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.Date;

public class IServiceHandler implements IService.Iface {

	@Override
	public GetTimeResp getTime(GetTimeReq req) throws TException {
		GetTimeResp resp = new GetTimeResp();
		resp.setTime(new Date().toString());
		resp.setStatus(new Status(Global.SUCCESS_CODE));
		return resp;
	}

	@Override
	public ConnectResp connect(ConnectReq req) throws TException {
		// TODO
		ConnectResp resp = new ConnectResp();
		String usr = req.username;
		String pwd = req.password;
		if (usr.equals(Global.USERNAME) && pwd.equals(Global.PASSWORD)) {
			ThssDB server = ThssDB.getInstance();
			resp.setSessionId(server.setSession());
			resp.setStatus(new Status(Global.SUCCESS_CODE));
		}
		else {
			resp.setStatus(new Status(Global.FAILURE_CODE));
		}
		return resp;
	}

	@Override
	public DisconnetResp disconnect(DisconnetReq req) throws TException {
		// TODO

		DisconnetResp resp = new DisconnetResp();

		ThssDB server = ThssDB.getInstance();
		boolean res = server.deleteSession(req.getSessionId());
		if(res)
			resp.setStatus(new Status(Global.SUCCESS_CODE));
		else
			resp.setStatus(new Status(Global.FAILURE_CODE));
		return resp;
	}

	@Override
	public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
		// TODO
		ThssDB thssDB = ThssDB.getInstance();
		ExecuteStatementResp resp = new ExecuteStatementResp();
		if (thssDB.checkSession(req.getSessionId())) {
//			TODO：进行查询，这个函数在client里面被调用，接着应该调用ThssDB里面的execute函数！
//				这里可以拿到sessionid！传给server就可以实现用户控制了！
//
//      SQLExecuteResult result = thssDB.execute(req.getStatement());
//      System.out.println("msg:"+result.getMessage());
//      resp.setMsg(result.getMessage());
//      resp.setStatus(new Status(result.isIsSucceed() ? Global.SUCCESS_CODE : Global.FAILURE_CODE));
//      resp.setIsAbort(result.isIsAbort());
//      resp.setHasResult(result.isHasResult());
//      if (result.isHasResult())
//      {
//        resp.setColumnsList(result.getColumnList());
//        resp.setRowList(result.getRowList());
//    }
		}
		else {
//      resp.setStatus(new Status(Global.FAILURE_CODE));
//      resp.setIsAbort(false);
//      resp.setHasResult(false);
//      resp.setMsg("Invalid session ID!");
		}
		resp.setStatus(new Status(Global.FAILURE_CODE));
		return resp;
	}
}

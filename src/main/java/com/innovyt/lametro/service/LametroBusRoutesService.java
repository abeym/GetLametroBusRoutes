package com.innovyt.lametro.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONException;
import org.json.JSONObject;

import com.innovyt.lametro.model.Stop;



@Path ("/routesservice/routes")
public class LametroBusRoutesService {
	
	protected static Connection conn=null;
	protected  static transient Configuration hbaseConf;
	protected static String zkQuorum="10.10.1.144";
	protected static String zkPort="2181";
	protected static String zkNodeParent="/hbase-unsecure";
	protected static final String LAMETRO_BUS_TABLE = "lametro_bus";
	protected static final String LAMETRO_STOPS_TABLE = "lametro_stops";
	
	protected static String LAMETRO_ROUTES_COLUMN_FAMILY_NAME = "bus_info";
	protected static String LAMETRO_STOPS_COLUMN_FAMILY_NAME = "stops_info";
	protected Configuration config ;
	protected HConnection connection;

	
	@Path("{bus_id}") 
	@GET 
	@Produces(MediaType.APPLICATION_JSON) 
	
	public String getRoutes(@PathParam("bus_id") String bus_id) throws IOException, JSONException{
		
		String busId=bus_id;
		config = constructConfiguration();
		connection = HConnectionManager.createConnection(config);
		HTableInterface  busTable = connection.getTable(LAMETRO_BUS_TABLE);
		JSONObject data= new JSONObject();
		
		Result res = doGet( busTable, busId);
		String route_id = Bytes.toString(res.getValue(Bytes.toBytes("LAMETRO_ROUTES_COLUMN_FAMILY_NAME"),Bytes.toBytes("route_id")));
		
	
				data.put("id", busId);
				data.put("route_id", route_id);
	

		return data.toString();
		
	}

	
	private List<Stop> getStops(String route_id) throws IOException, JSONException{
		
		String routeId=route_id;
		HTableInterface  stopTable = connection.getTable(LAMETRO_STOPS_TABLE);
		ResultScanner resultScanner = stopTable.getScanner(Bytes.toBytes(LAMETRO_ROUTES_COLUMN_FAMILY_NAME));
		
		List<Stop> stopsList = new ArrayList<Stop>();
		for(Result result = resultScanner.next(); result != null ; result = resultScanner.next()) {
			//
			int stopId = Bytes.toInt(result.getValue(Bytes.toBytes(LAMETRO_ROUTES_COLUMN_FAMILY_NAME), Bytes.toBytes("id")));
			String latitude = Bytes.toString(result.getValue(Bytes.toBytes(LAMETRO_ROUTES_COLUMN_FAMILY_NAME), Bytes.toBytes("latitude")));
			String longitude = Bytes.toString(result.getValue(Bytes.toBytes(LAMETRO_ROUTES_COLUMN_FAMILY_NAME), Bytes.toBytes("longitude")));
			String display_name = Bytes.toString(result.getValue(Bytes.toBytes(LAMETRO_ROUTES_COLUMN_FAMILY_NAME), Bytes.toBytes("display_name")));
			//
			Stop stop = new Stop();
			stop.setDisplay_name(display_name);
			stop.setLatitude(latitude);
			stop.setLongitude(longitude);
			stop.setId(stopId);
			//
			stopsList.add(stop);
		}
		
	
		return stopsList;
		
	}

	
	private Configuration constructConfiguration() {
		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum",zkQuorum);
		config.set("hbase.zookeeper.property.clientPort",zkPort);
		config.set("zookeeper.znode.parent", zkNodeParent);
		config.set("hbase.defaults.for.version.skip", "true");
		return config;
	}

	
	private Result doGet(HTableInterface table, String columnId) {

		Get g = new Get(Bytes.toBytes(columnId));
	
		try {
			return table.get(g);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	

}

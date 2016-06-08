/*
 * Copyright (C) 2011 LINUXTEK, Inc.  All Rights Reserved.
 */
package com.linuxtek.kona.zipCode.service.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.linuxtek.kona.zipCode.dao.KZipCodeDao;
import com.linuxtek.kona.zipCode.entity.KZipCode;
import com.linuxtek.kona.zipCode.service.KZipCodeService;

public class KZipCodeServiceImpl implements KZipCodeService {
	private static Logger logger = Logger.getLogger(KZipCodeServiceImpl.class);

	private static HashMap<Integer, String> _zipList5Cache = new HashMap<Integer, String>();
	private static HashMap<Integer, String> _zipList10Cache = new HashMap<Integer, String>();
	private static HashMap<Integer, String> _zipList25Cache = new HashMap<Integer, String>();
	private static HashMap<Integer, String> _zipList50Cache = new HashMap<Integer, String>();
	private static HashMap<Integer, String> _zipList100Cache = new HashMap<Integer, String>();
	private static HashMap<Integer, String> _zipList250Cache = new HashMap<Integer, String>();

	private final static int EARTH_RADIUS = 3956;

	private static HashMap<Integer,KZipCode> zipMap = null;
	private static ArrayList<Integer> calculatedZipRadii = null;
    
    @Autowired
    private KZipCodeDao zipCodeDao;

	// Needed to initialize the class from outside
	public static void init() throws SQLException {
		initZipMap();
	}

	/* (non-Javadoc)
	 * @see com.linuxtek.kona.service.impl.KZipCodeService#isValid(int)
	 */
	@Override
	public boolean isValid(Integer zip) {
		return (isValid(zip));
	}

	public static boolean isValid(int zip) throws SQLException {
		initZipMap();

		KZipCode zipcode = (KZipCode) zipMap.get(new Integer(zip));

		if (zipcode != null)
			return (true);

		return (false);
	}

	private static void initZipMap() {
		/*
		if (zipMap != null)
			return;

		logger.info("KZipData: initZipMap: initializing zipMap ...");

		zipMap = new HashMap<Integer,KZipCode>();

		Connection conn = ds.getConnection();
		KStatement stmt = null;
		ResultSet rs = null;
		String sql = null;

		try {
			sql = "SELECT	zipcode,			\n" + "			longitude,		\n"
					+ "			latitude			\n" + "	FROM	KZip.ZipData	\n";

			stmt = getStatement(conn);
			rs = stmt.executeQuery(sql);

			while (rs.next()) {
				int zipcode = rs.getInt(1);
				double longitude = rs.getDouble(2);
				double latitude = rs.getDouble(3);

				zipMap.put(new Integer(zipcode), new KZipCode(zipcode,
						longitude, latitude));
			}

			logger.info("KZipData: initZipMap: zipMap initialized: entries = "
					+ zipMap.size());

			rs.close();
			stmt.close();

			// Check if we have a KZip.ZipRadius table populated -- use 19102 as
			// reference zip so we don't have to search the entire table!
			sql = "SELECT	DISTINCT radius	\n" + "	FROM	KZip.ZipRadius		\n"
					+ " WHERE	origin = 19102		\n";

			stmt = getStatement(conn);
			rs = stmt.executeQuery(sql);

			calculatedZipRadii = new ArrayList<Integer>();
			while (rs.next())
				calculatedZipRadii.add(new Integer(rs.getInt(1)));

			logger.info("KZipData: initZipMap: calculatedZipRadii initialized = "
					+ calculatedZipRadii.size() + " entries");

			return;
		} finally {
			// conn.close();
		}
		*/
	}

	private static KZipCode initZipCode(KZipCode zip)
			throws SQLException {
		if (zip == null)
			throw new NullPointerException("KZipCode object is null");

		KZipCode zipcode = null;

		if ((zip.getLatitude() == -1.0) || (zip.getLongitude() == -1.0)) {
			initZipMap();

			zipcode = (KZipCode) zipMap.get(new Integer(zip.intValue()));

			if (zipcode == null)
				throw new NullPointerException(
						"KZipCode object not found for: " + zip.intValue());
		} else
			zipcode = zip;

		return (zipcode);
	}

	/* (non-Javadoc)
	 * @see com.linuxtek.kona.service.impl.KZipCodeService#getDistance(com.linuxtek.kona.entity.KZipCode, com.linuxtek.kona.entity.KZipCode)
	 */
	@Override
	public Double getDistance(KZipCode zip1, KZipCode zip2) {
		return getDistance(zip1.intValue(), zip2.intValue());
	}

	/* (non-Javadoc)
	 * @see com.linuxtek.kona.service.impl.KZipCodeService#getDistance(int, int)
	 */
	@Override
	public Double getDistance(Integer zip1, Integer zip2) {
		initZipMap();

		KZipCode z1 = (KZipCode) zipMap.get(new Integer(zip1));

		if (z1 == null)
			throw new NullPointerException("KZipCode object not found in "
					+ "map for: " + zip1);

		KZipCode z2 = (KZipCode) zipMap.get(new Integer(zip2));

		if (z2 == null)
			throw new NullPointerException("KZipCode object not found in "
					+ "map for: " + zip2);

		return (greatCircleDistance(z1.getLatitude(), z1.getLongitude(),
				z2.getLatitude(), z2.getLongitude()));
	}

	/* (non-Javadoc)
	 * @see com.linuxtek.kona.service.impl.KZipCodeService#areZipsInRadius(com.linuxtek.kona.entity.KZipCode, com.linuxtek.kona.entity.KZipCode, int)
	 */
	@Override
	public boolean areZipsInRadius(KZipCode zip1, KZipCode zip2, int radius)
			throws SQLException {
		return (areZipsInRadius(zip1.intValue(), zip2.intValue(), radius));
	}

	public static boolean areZipsInRadius(KDataSource ds, KZipCode zip1,
			KZipCode zip2, int radius) throws SQLException {
		return (areZipsInRadius(ds, zip1.intValue(), zip2.intValue(), radius));
	}

	/* (non-Javadoc)
	 * @see com.linuxtek.kona.service.impl.KZipCodeService#areZipsInRadius(int, int, int)
	 */
	@Override
	public boolean areZipsInRadius(int zip1, int zip2, int radius)
			throws SQLException {
		return (areZipsInRadius(ds, zip1, zip2, radius));
	}

	public static boolean areZipsInRadius(KDataSource ds, int zip1, int zip2,
			int radius) throws SQLException {
		int[] result = getZipsInRadius(ds, zip1, radius);

		if (result == null)
			throw new NullPointerException("getZipsInRadius() returned null "
					+ "for: " + zip1);

		for (int i = 0; i < result.length; i++) {
			if (result[i] == zip2)
				return (true);
		}
		return (false);
	}

	/* (non-Javadoc)
	 * @see com.linuxtek.kona.service.impl.KZipCodeService#getZipsInRadius(com.linuxtek.kona.entity.KZipCode, int)
	 */
	@Override
	public int[] getZipsInRadius(KZipCode zip, int radius) throws SQLException {
		return (getZipsInRadius(ds, zip, radius));
	}

	public static int[] getZipsInRadius(KDataSource ds, KZipCode zip, int radius)
			throws SQLException {
		return (getZipsInRadius(ds, zip.intValue(), radius));
	}

	/* (non-Javadoc)
	 * @see com.linuxtek.kona.service.impl.KZipCodeService#getZipsInRadius(int, int)
	 */
	@Override
	public int[] getZipsInRadius(int zip, int radius) throws SQLException {
		return (getZipsInRadius(ds, zip, radius));
	}

	public static int[] getZipsInRadius(KDataSource ds, int zip, int radius)
			throws SQLException {
		if (!isValid(ds, zip))
			throw new IllegalArgumentException("Invalid zipcode: " + zip);

		if (radius < 0)
			throw new IllegalArgumentException(
					"Invalid radius: value cannot be negative");

		Connection conn = ds.getConnection();
		KPreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;

		initZipMap(ds);

		try {
			// logger.debug("KZipData: checking calculatedZipRadii ... ");
			// First check if what we're looking for is in the KZipRadius table
			if (calculatedZipRadii != null
					&& calculatedZipRadii.contains(new Integer(radius))) {
				// logger.debug("KZipData: using calculatedZipRadii!");
				sql = "SELECT	destination		\n" + "	FROM	KZip.ZipRadius	\n"
						+ " WHERE	origin = ? 		\n" + "	 AND	radius = ? 		\n";

				pstmt = getPreparedStatement(conn, sql);
				pstmt.setInt(1, zip);
				pstmt.setInt(2, radius);

				rs = pstmt.executeQuery();

				int rowCount = getRowCount(rs);
				int[] result = new int[rowCount];

				int i = 0;
				while (rs.next())
					result[i++] = rs.getInt(1);

				// logger.debug("KZipData: getZipsInRadius: KZipRadius: found ["
				// +
				// rowCount + "] zipcodes [" + radius + "] miles of: " + zip);

				return (result);
			}

			logger.info("KZipData: calculatedZipRadii does not contain: "
					+ radius);

			// OK, we don't have a radius table or the radius we're searching on
			// is not pre-calculated. Figure it out manually ...

			KZipCode zipcode = (KZipCode) zipMap.get(new Integer(zip));

			if (zipcode == null)
				throw new NullPointerException(
						"KZipCode object not found in map " + "for: " + zip);

			String clause = "(POW((69.1 * (longitude - ?) * cos(? / 57.3)), 2) + "
					+ "POW((69.1 * (latitude - ?)), 2)) < (? * ?)";

			sql = "SELECT	zipcode			\n" + "	FROM	KZip.ZipData	\n" + " WHERE	"
					+ clause + "	\n";

			pstmt = getPreparedStatement(conn, sql);
			pstmt.setDouble(1, zipcode.getLongitude());
			pstmt.setDouble(2, zipcode.getLatitude());
			pstmt.setDouble(3, zipcode.getLatitude());
			pstmt.setInt(4, radius);
			pstmt.setInt(5, radius);

			rs = pstmt.executeQuery();

			int rowCount = getRowCount(rs);
			int[] result = new int[rowCount];

			int i = 0;
			while (rs.next()) {
				result[i] = rs.getInt(1);
				i += 1;
			}

			// logger.debug("KZipData: getZipsInRadius: found [" + rowCount +
			// "] zipcodes [" + radius + "] miles of: " + zip);

			return (result);
		} finally {
			// conn.close();
		}
	}

	public static double greatCircleDistance(double lat1, double long1,
			double lat2, double long2) {
		double temp = 0.0;
		double deltaLong = 0.0;
		double deltaLat = 0.0;
		double distance = 0.0;

		/* Convert all the degrees to radians */
		lat1 = Math.toRadians(lat1);
		long1 = Math.toRadians(long1);
		lat2 = Math.toRadians(lat2);
		long2 = Math.toRadians(long2);

		/* Find the deltas */
		deltaLat = lat2 - lat1;
		deltaLong = long2 - long1;

		/* Find the GC distance */
		temp = Math.pow(Math.sin(deltaLat / 2.0), 2) + Math.cos(lat1)
				* Math.cos(lat2) * Math.pow(Math.sin(deltaLong / 2.0), 2);

		distance = EARTH_RADIUS * 2
				* Math.atan2(Math.sqrt(temp), Math.sqrt(1 - temp));

		return (distance);
	}

	public void initKZipRadiusTable(int radius) throws SQLException {
		initKZipRadiusTable(ds, radius);
	}

	public static void initKZipRadiusTable(KDataSource ds, int radius)
			throws SQLException {
		Runtime r = Runtime.getRuntime();

		Connection conn = ds.getConnection();
		KPreparedStatement pstmt, pstmt1 = null;
		ResultSet rs = null;
		String sql, sql1 = null;
		int rowCount = 0;

		initZipMap(ds);

		try {
			// Delete the current entries for this zipcode
			sql = "DELETE							\n" + "  FROM	KZip.ZipRadius		\n"
					+ " WHERE	radius = ?			\n";

			pstmt = getPreparedStatement(conn, sql);
			pstmt.setInt(1, radius);
			pstmt.executeUpdate();

			String clause = "(POW((69.1 * (longitude - ?) * cos(? / 57.3)), 2) + "
					+ "POW((69.1 * (latitude - ?)), 2)) < (? * ?)";

			int zipCount = 0;
			int totalInserts = 0;
			Iterator it = zipMap.values().iterator();
			while (it.hasNext()) {
				KZipCode zipcode = (KZipCode) it.next();

				int origin = zipcode.intValue();

				// Get the matching zips for this KZipCode
				sql = "SELECT	zipcode			\n" + "	FROM	KZip.ZipData	\n"
						+ " WHERE	" + clause + "	\n";

				pstmt = getPreparedStatement(conn, sql);
				pstmt.setDouble(1, zipcode.getLongitude());
				pstmt.setDouble(2, zipcode.getLatitude());
				pstmt.setDouble(3, zipcode.getLatitude());
				pstmt.setInt(4, radius);
				pstmt.setInt(5, radius);

				rs = pstmt.executeQuery();

				int count = 0;
				while (rs.next()) {
					int destination = rs.getInt(1);

					sql1 = "INSERT							\n" + " INTO	KZip.ZipRadius	\n"
							+ " 		(	origin,			\n" + " 			radius,			\n"
							+ " 			destination)	\n" + "VALUES	(?, ?, ?)			\n";

					pstmt1 = getPreparedStatement(conn, sql1);
					pstmt1.setInt(1, origin);
					pstmt1.setInt(2, radius);
					pstmt1.setInt(3, destination);

					rowCount = pstmt1.executeUpdate();

					if (rowCount != 1)
						throw new SQLException(
								"Error inserting record into "
										+ "KZip.ZipRadius: Expected rowCount = 1, got rowCount = "
										+ "Expected rowCount = 1, got rowCount = "
										+ rowCount);

					count++;
					totalInserts++;

					// end by collecting garbage
					// if (KDBC.getObjectCount() > 500)
					if (totalInserts % 50000 == 0) {
						r.gc();
						// logger.debug("Total Mem (MB): " +
						// r.totalMemory()/1024/1024 +
						// "\t Free Mem (MB): " + r.freeMemory()/1024/1024 +
						// "\t Object Count: " + KDBC.getObjectCount());
					}
				}

				// logger.debug("Records inserted for ["+origin+"] = " + count);

				zipcode = null;
				zipCount++;
			}

			logger.debug("Origins processed: " + zipCount);
			logger.debug("Destinations inserted : " + totalInserts);
		} finally {
			// conn.close();
		}
	}

	public static String getZipList(String zip, int radius)
			throws SQLException {
		try {
			int z = Integer.parseInt(zip);
			return (getZipList(z, radius));
		} catch (NumberFormatException e) {
			logger.error("KZipData.getZipList():", e);
			return ("(false)");
		}
	}

	public static String getZipList(int zip, int radius)
			throws SQLException {
		String zipList = null;

		if (radius < 0)
			return ("(false)");

		switch (radius) {
		case 5:
			zipList = (String) _zipList5Cache.get(zip);
			if (zipList == null) {
				zipList = createZipList(zip, radius);
				_zipList5Cache.put(zip, zipList);
			}
			break;

		case 10:
			zipList = (String) _zipList10Cache.get(zip);
			if (zipList == null) {
				zipList = createZipList(zip, radius);
				_zipList10Cache.put(zip, zipList);
			}
			break;

		case 25:
			zipList = (String) _zipList25Cache.get(zip);
			if (zipList == null) {
				zipList = createZipList(zip, radius);
				_zipList25Cache.put(zip, zipList);
			}
			break;

		case 50:
			zipList = (String) _zipList50Cache.get(zip);
			if (zipList == null) {
				zipList = createZipList(zip, radius);
				_zipList50Cache.put(zip, zipList);
			}
			break;

		case 100:
			zipList = (String) _zipList100Cache.get(zip);
			if (zipList == null) {
				zipList = createZipList(zip, radius);
				_zipList100Cache.put(zip, zipList);
			}

		case 250:
			zipList = (String) _zipList250Cache.get(zip);
			if (zipList == null) {
				zipList = createZipList(zip, radius);
				_zipList250Cache.put(zip, zipList);
			}
		default:
			zipList = createZipList(zip, radius);
		}
		return (zipList);
	}

	private static String createZipList(Integer zip, Integer radius) {
		int[] zips = getZipsInRadius(zip, radius);

		if (zips == null || zips.length == 0)
			return ("(false)");

		String zipList = "(";
		for (int i = 0; i < zips.length; i++)
			zipList += zips[i] + ",";

		zipList = zipList.substring(0, zipList.length() - 1);
		zipList += ")";

		return (zipList);
	}

	/*
	 * ------------------------------------------------ Use main to allow a user
	 * to initialize the KZip.ZipData and KZip.ZipRadius tables. Maybe create
	 * command line and gui front-ends.
	 * 
	 * public static void main(String[] args) { String brokerParamFile =
	 * args[0];
	 * 
	 * KBroker broker = new KBroker(brokerParamFile); KDataSource ds =
	 * broker.getDataSource();
	 * 
	 * KZipData zipData = new KZipData(ds);
	 * 
	 * System.out.println("populate KZipRadius for 250 miles ...");
	 * zipData.initKZipRadiusTable(250);
	 * 
	 * ds.close(); System.exit(0); }
	 * ------------------------------------------------
	 */

}

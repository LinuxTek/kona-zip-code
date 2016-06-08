/*
 * Copyright (C) 2011 LINUXTEK, Inc.  All Rights Reserved.
 */
package com.linuxtek.kona.zipCode.dao;

import java.util.List;

import com.linuxtek.kona.data.dao.KMyBatisDao;
import com.linuxtek.kona.zipCode.entity.KZipCode;

public interface KZipCodeDao extends KMyBatisDao {
    public List<KZipCode> fetchAll();
    public List<KZipCode> fetchInRadius(Integer zipCode, Integer radius);
    public void initRadiusTable(Integer radius);
}


/*

	private static void initZipMap(KDataSource ds) throws SQLException {
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
*/


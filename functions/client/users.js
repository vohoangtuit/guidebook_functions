import { onRequest } from "firebase-functions/v2/https";
import { BigQuery } from "@google-cloud/bigquery";

const bigquery = new BigQuery();
const appId = "guidebook-585c9";
const dataset = "tracking";
const table_sessions = "sessions";
const table_sign_in = "sign_in";
const table_location = "location_guide";


export const getSessions = onRequest({ region: "us-central1" }, async (req, res) => {
    res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "POST");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(204).send("");

  try {
    const { startDate, endDate, page = 1, limit = 20 } = req.body;
    if (!startDate || !endDate) {
      return res.status(400).json({ error: "Thiếu startDate hoặc endDate" });
    }

    const offset = (page - 1) * limit;

    // Step 1: Tổng số user (distinct username)
    const totalUserQuery = `
      SELECT COUNT(DISTINCT username) AS totalUser
      FROM \`${appId}.${dataset}.${table_sessions}\`
      WHERE PARSE_DATE('%Y-%m-%d', yearMonthDay) BETWEEN "${startDate}" AND "${endDate}"
        AND username IS NOT NULL
    `;
    const [totalUserResult] = await bigquery.query({ query: totalUserQuery });
    const totalUser = totalUserResult[0]?.totalUser || 0;

    // Step 2: Đếm theo deviceType
    const deviceQuery = `
      SELECT deviceType, COUNT(DISTINCT username) AS total
      FROM \`${appId}.${dataset}.${table_sessions}\`
      WHERE PARSE_DATE('%Y-%m-%d', yearMonthDay) BETWEEN "${startDate}" AND "${endDate}"
        AND username IS NOT NULL
      GROUP BY deviceType
    `;
    const [deviceRows] = await bigquery.query({ query: deviceQuery });
    let android = 0, ios = 0;
    for (const row of deviceRows) {
      if (row.deviceType === "ANDROID") android = row.total;
      if (row.deviceType === "IOS") ios = row.total;
    }

    // Step 3: Get list of user summary
    const summaryQuery = `
      SELECT
          username,
        ANY_VALUE(fullName) AS fullName,
        COUNT(*) AS sessionCount,
        SUM(duration) AS totalDuration
      FROM \`${appId}.${dataset}.${table_sessions}\`
      WHERE PARSE_DATE('%Y-%m-%d', yearMonthDay) BETWEEN "${startDate}" AND "${endDate}"
        AND username IS NOT NULL
      GROUP BY username
      ORDER BY totalDuration DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;

    const [summaryRows] = await bigquery.query({ query: summaryQuery });
    const results = [];

    // Step 4: Get list of sessions for each user
    for (const row of summaryRows) {
      const sessionQuery = `
        SELECT
          username,
          fullName,
          duration,
          yearMonthDay,
          yearMonthDayHHMMSS,
          deviceType
        FROM \`${appId}.${dataset}.${table_sessions}\`
        WHERE username = @username
          AND PARSE_DATE('%Y-%m-%d', yearMonthDay) BETWEEN "${startDate}" AND "${endDate}"
        ORDER BY yearMonthDay DESC
      `;

      const options = {
        query: sessionQuery,
        params: { username: row.username },
      };

      const [sessions] = await bigquery.query(options);

      results.push({
          username: row.username,
        fullName: row.fullName,
        sessionCount: row.sessionCount,
        totalDuration: row.totalDuration,
        list: sessions
      });
    }

    return res.json({
      success: true,
      message: "📋 Lấy danh sách sessions thành công!",
      data: {
        totalUser,
        android,
        ios,
        users: results
      }
    });
  } catch (err) {
    console.error("❌ BigQuery error:", err.message);
    return res.status(500).json({
      success: false,
      message: "Lỗi khi truy vấn sessions",
      data: {}
    });
  }
});

export const getSignInAccount = onRequest({ region: "us-central1" }, async (req, res) => {
    res.set("Access-Control-Allow-Origin", "*");
    res.set("Access-Control-Allow-Methods", "POST");
    res.set("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") return res.status(204).send("");

    try {
        const { startDate, endDate, page = 1, limit = 50 } = req.body;

        if (!startDate || !endDate) {
            return res.status(400).json({ success: false, message: "Thiếu startDate hoặc endDate" });
        }

        const offset = (page - 1) * limit;

        const query = `
      SELECT
          yearMonthDay,
          username,
          deviceName,
          devicePlatform,
          yearMonthDayHHMM
      FROM \`${appId}.${dataset}.${table_sign_in}\`
      WHERE PARSE_DATE('%Y-%m-%d', yearMonthDay) BETWEEN "${startDate}" AND "${endDate}"
      ORDER BY yearMonthDay DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `;
        const [rows] = await bigquery.query({ query });

        return res.json({
            success: true,
            message: "📋 Lấy danh sách SignIn thành công!",
            data: {
                list: rows
            }
        });

    } catch (err) {
        console.error("❌ BigQuery error:", err.message);
        return res.status(500).json({
            success: false,
            message: "Lỗi khi truy vấn dữ liệu SignIn",
            data: {}
        });
    }
});

export const getLocationGuide = onRequest({ region: "us-central1" }, async (req, res) => {
    res.set("Access-Control-Allow-Origin", "*");
    res.set("Access-Control-Allow-Methods", "POST");
    res.set("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") return res.status(204).send("");

    try {
        const { startDate, endDate, page = 1, limit = 50 } = req.body;

        if (!startDate || !endDate) {
            return res.status(400).json({ success: false, message: "Thiếu startDate hoặc endDate" });
        }

        const offset = (page - 1) * limit;

        const query = `
            SELECT
                username,
                yearMonthDay,
                fullName,
                latitude,
                longitude,
                location,
                deviceName,
                devicePlatform,
                yearMonthDayHHMM,
               \`timestamp\` 
            FROM \`${appId}.${dataset}.${table_location}\`
            WHERE PARSE_DATE('%Y-%m-%d', yearMonthDay)
                      BETWEEN "${startDate}" AND "${endDate}"
            ORDER BY \`timestamp\`  DESC
                LIMIT ${limit}
            OFFSET ${offset}
        `;
        const [rows] = await bigquery.query({ query });

        return res.json({
            success: true,
            message: "📋 Lấy danh sách Vị trí Guide thành công!",
            data: {
                list: rows
            }
        });

    } catch (err) {
        console.error("❌ BigQuery error:", err.message);
        return res.status(500).json({
            success: false,
            message: "Lỗi khi truy vấn dữ liệu Vị trí",
            data: {}
        });
    }
});
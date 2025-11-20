import { onRequest } from "firebase-functions/v2/https";
import { BigQuery } from "@google-cloud/bigquery";

const bigquery = new BigQuery();
const appId = "guidebook-585c9";
const dataset = "tracking";
const table_complete_report = "complete_report";
 const table_confirm_tour = "confirm_tour";
 const table_reject_tour = "reject_tour";

export const getCompleteReport = onRequest({ region: "us-central1" }, async (req, res) => {
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
                tourId,
                tourCode,
                reportId,
                deviceName,
                devicePlatform,
                yearMonthDayHHMM,
               \`timestamp\` 
            FROM \`${appId}.${dataset}.${table_complete_report}\`
            WHERE PARSE_DATE('%Y-%m-%d', yearMonthDay)
                      BETWEEN "${startDate}" AND "${endDate}"
            ORDER BY \`timestamp\`  DESC
                LIMIT ${limit}
            OFFSET ${offset}
        `;
        const [rows] = await bigquery.query({ query });

        return res.json({
            success: true,
            message: "📋 Lấy danh sách CompleteReport thành công!",
            data: {
                list: rows
            }
        });

    } catch (err) {
        console.error("❌ BigQuery error:", err.message);
        return res.status(500).json({
            success: false,
            message: "Lỗi khi truy vấn dữ liệu CompleteReport",
            data: {}
        });
    }
});

export const getConfirmTour = onRequest({ region: "us-central1" }, async (req, res) => {
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
                fullName,
                tourId,
                tourCode,
                deviceName,
                devicePlatform,
                yearMonthDayHHMM,
               \`timestamp\` 
            FROM \`${appId}.${dataset}.${table_confirm_tour}\`
            WHERE PARSE_DATE('%Y-%m-%d', yearMonthDay)
                      BETWEEN "${startDate}" AND "${endDate}"
            ORDER BY \`timestamp\`  DESC
                LIMIT ${limit}
            OFFSET ${offset}
        `;
        const [rows] = await bigquery.query({ query });

        return res.json({
            success: true,
            message: "📋 Lấy danh sách Confirm Tour thành công!",
            data: {
                list: rows
            }
        });

    } catch (err) {
        console.error("❌ BigQuery error:", err.message);
        return res.status(500).json({
            success: false,
            message: "Lỗi khi truy vấn dữ liệu Confirm Tour",
            data: {}
        });
    }
});
export const getRejectTour = onRequest({ region: "us-central1" }, async (req, res) => {
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
                fullName,
                tourId,
                tourCode,
                reason,
                deviceName,
                devicePlatform,
                yearMonthDayHHMM,
               \`timestamp\` 
            FROM \`${appId}.${dataset}.${table_reject_tour}\`
            WHERE PARSE_DATE('%Y-%m-%d', yearMonthDay)
                      BETWEEN "${startDate}" AND "${endDate}"
            ORDER BY \`timestamp\`  DESC
                LIMIT ${limit}
            OFFSET ${offset}
        `;
        const [rows] = await bigquery.query({ query });

        return res.json({
            success: true,
            message: "📋 Lấy danh sách Reject Tour thành công!",
            data: {
                list: rows
            }
        });

    } catch (err) {
        console.error("❌ BigQuery error:", err.message);
        return res.status(500).json({
            success: false,
            message: "Lỗi khi truy vấn dữ liệu Reject Tour",
            data: {}
        });
    }
});
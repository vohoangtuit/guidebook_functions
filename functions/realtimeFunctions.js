import {onRequest} from "firebase-functions/v2/https";
import {onValueCreated} from "firebase-functions/v2/database";
import {setGlobalOptions} from "firebase-functions/v2";
import {BigQuery} from "@google-cloud/bigquery";
import {tableNameMap} from "./constants.js";


const bigquery = new BigQuery();
setGlobalOptions({region: "us-central1", timeoutSeconds: 300});

const datasetId = "tracking";
//const urlInsert = `https://us-central1-guidebook-585c9.cloudfunctions.net/insertToBigQuery`;

export const insertToBigQuery = onRequest(async (req, res) => {
    const data = req.body;
    const {key, tableName, date} = data;

    console.log("📨 Payload nhận được:", data);
    console.log("✅ tableName:", tableName);
    // console.log("✅ Có trong tableNameMap:", !!tableNameMap[tableName]);

    if (!data || !tableName || !tableNameMap[tableName]) {
        console.log(`❌ Missing data or unknown table:`, data);
        return res.status(400).send("Invalid request");
    }

    const bqTable = tableNameMap[tableName];

    const row = {
        key,
        yearMonthDay: date,
        ...data
    };

    delete row.tableName;
    delete row.date;

    Object.keys(row).forEach((k) => {
        if (row[k] === undefined) row[k] = null;
    });

    //console.log("📦 Row chuẩn bị insert vào BigQuery:", row);

    try {
        await bigquery.dataset(datasetId).table(bqTable).insert(row);
        console.log(`✅ Inserted into ${bqTable}: ${key}`);
        res.status(200).send("Inserted");
    } catch (err) {
        // console.error(`❌ Failed insert:`, err);

        if (err.name === "PartialFailureError" && err.errors) {
            err.errors.forEach((e, i) => {
                console.error(`➡️ Lỗi dòng ${i}:`, JSON.stringify(e.errors));
                console.error(`➡️ Dữ liệu lỗi dòng ${i}:`, JSON.stringify(e.row));
            });
        }

        res.status(500).send("Insert failed");
    }
});
// Path lưu giống nhau nên gôm chung
export const realtimeToBigQuery = onValueCreated(
    "/Database/{tableName}/{userid}/{date}/{pushId}",
    async (event) => {
        const eventId = event.id; // v2 event ID
        const data = event.data.val();
        const key = event.data.key;
        const {tableName} = event.params;
        const bqTable = tableNameMap[tableName];
        const row = {
            eventId,
            key,
            ...data
        };

        try {
            await bigquery.dataset(datasetId).table(bqTable).insert([{key: eventId, ...row}]);

        } catch (err) {
            console.error("❌ Failed insert:", err);
            if (err.name === "PartialFailureError") {
                err.errors?.forEach((e) => {
                    console.error("➡️ BigQuery error:", e.errors);
                    console.error("➡️ Row:", e.row);
                });
            }
        }
    }
);
/// Sesstion thêm 1 cấo nữa nên làm riêng
export const realtimeSession = onValueCreated(
    "/Database/Sessions/{date}/{userId}/{pushId}",
    async (event) => {

        const {pushId} = event.params;

        const data = event.data.val();

        try {
            await bigquery
                .dataset(datasetId)
                .table("sessions")
                .insert([{key:pushId,...data}], {
                    insertId: pushId,
                    skipInvalidRows: false,
                    ignoreUnknownValues: false,
                });
           // console.log("✅ Inserted into BigQuery:", row.key);
        } catch (err) {
            // console.error("❌ BigQuery Error:", err);
            if (err.name === "PartialFailureError") {
                err.errors?.forEach((e) => {
                    console.error("➡️ BigQuery error:", e.errors);
                    console.error("➡️ Row:", e.row);
                });
            }
        }
    }
);
/// LocationGuide thêm 1 cấo nữa nên làm riêng

export const realtimeLocationGuide = onValueCreated(
    "/Database/LocationGuide/{userId}/{date}/{pushId}",
    async (event) => {
        // const { userId, date, pushId } = event.params;
        const {pushId} = event.params;

        const data = event.data.val();

        try {
            await bigquery
                .dataset(datasetId)
                .table("location_guide")
                .insert([{key:pushId,...data}],
                    {
                        insertId: pushId, skipInvalidRows: false,
                        ignoreUnknownValues: false,
                    })
            //  console.log("✅ Inserted into BigQuery:", row.key);
        } catch (err) {
            if (err.name === "PartialFailureError") {
                err.errors?.forEach((e) => {
                    console.error("➡️ BigQuery error:", e.errors);
                    console.error("➡️ Row:", e.row);
                });
            }
        }
    }
);


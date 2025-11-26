import {onRequest} from "firebase-functions/v2/https";
import {onValueCreated} from "firebase-functions/v2/database";
import {setGlobalOptions} from "firebase-functions/v2";
import {BigQuery} from "@google-cloud/bigquery";
import {tableNameMap} from "./constants.js";


const bigquery = new BigQuery();
setGlobalOptions({region: "us-central1", timeoutSeconds: 300});

const datasetId = "tracking";

// Path lưu giống nhau nên gôm chung
export const realtimeToBigQuery = onValueCreated(
    "/Database/{tableName}/{userid}/{date}/{pushId}",
    async (event) => {
        const {pushId,tableName} = event.params;
        const data = event.data.val();
        const bqTable = tableNameMap[tableName];

        try {
            await bigquery.dataset(datasetId).table(bqTable).insert([{key: pushId, ...data}],{
                insertId: pushId,
                skipInvalidRows: false,
                ignoreUnknownValues: false,
            });

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


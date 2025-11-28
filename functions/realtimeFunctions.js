import {onValueCreated} from "firebase-functions/v2/database";
import {setGlobalOptions} from "firebase-functions/v2";
import {BigQuery} from "@google-cloud/bigquery";
import {tableNameMap} from "./constants.js";
//import {checkKeyExists} from "./validate/util.js";


const bigquery = new BigQuery();
setGlobalOptions({region: "us-central1", timeoutSeconds: 300});

const datasetId = "tracking";
//const fieldKey = "key";

// Path lưu giống nhau nên gôm chung
export const realtimeToBigQuery = onValueCreated(
    "/Database/{tableName}/{userid}/{date}/{pushId}",
    async (event) => {

        const {pushId,tableName} = event.params;
        const data = event.data.val();
       // if (!data.key) return;

        const bqTable = tableNameMap[tableName];
        // todo duplicate insert, recheck
        // const exists = await checkKeyExists(datasetId, bqTable, fieldKey, pushId);
        // if (exists) {
        //     console.log("⛔",bqTable," -  Skip duplicate insert:", pushId);
        //     return;
        // }
        const payload = {
            key: pushId,  // luôn ép = pushId, tránh xung đột
            ...data,
        };
        try {
            await bigquery.dataset(datasetId).table(bqTable).insert(payload,{
                insertId: pushId,
            });

        } catch (err) {
           // console.error("❌ Failed insert:", err);
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

        const tableName = "sessions";
        // todo duplicate insert, recheck
        //if (!data.key) return;
        // const exists = await checkKeyExists(datasetId, tableName, fieldKey, pushId);
        // if (exists) {
        //     console.log("⛔ Sessions Skip duplicate insert:", pushId);
        //     return;
        // }
        const payload = {
            key: pushId,  // luôn ép = pushId, tránh xung đột
            ...data,
        };
        try {
            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert(payload,{
                    insertId: pushId,   // chống insert 2 lần
                });
            console.log("✅ Inserted:", pushId);
        } catch (err) {
            if (err.name === "PartialFailureError") {
                console.error("BIGQUERY PARTIAL FAILURE:", JSON.stringify(err, null, 2));
                // err.errors?.forEach((e) => {
                //     console.error("➡️ BigQuery error:", e.errors);
                //     console.error("➡️ Row:", e.row);
                // });
            }
        }
    }
);

export const realtimeLocationGuide = onValueCreated(
    "/Database/LocationGuide/{userId}/{date}/{pushId}",
    async (event) => {

        const {pushId} = event.params;
        const tableName ="location_guide";
        const data = event.data.val();

        // todo duplicate insert, recheck

        // const exists = await checkKeyExists(datasetId, tableName, fieldKey, pushId);
        // if (exists) {
        //     console.log("⛔ LocationGuide Skip duplicate insert:", pushId);
        //     return;
        // }
        const payload = {
            key: pushId,
            ...data,
        };
        try {
            await bigquery
                .dataset(datasetId)
                .table(tableName)
                .insert(payload,{
                    insertId: pushId,   // chống insert 2 lần
                })
              //console.log("✅ Inserted into BigQuery:", pushId);
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



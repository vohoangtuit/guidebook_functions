import {onValueCreated} from "firebase-functions/v2/database";
import {setGlobalOptions} from "firebase-functions/v2";
import {BigQuery} from "@google-cloud/bigquery";
import {tableNameMap} from "./constants.js";
//import {checkKeyExists} from "./validate/util.js";


const bigquery = new BigQuery();
setGlobalOptions({region: "us-central1", timeoutSeconds: 300});

const datasetId = "tracking";
export async function insertRow(tableName, payload, insertId) {
    const id = String(insertId || "");
    const table = bigquery.dataset(datasetId).table(tableName);

    if (!id) {
        console.warn("⚠ insertRow: insertId is empty", { insertId });
    }

    const rows = [
        {
            insertId: id,   // chống duplicate
            json: payload,  // đúng format chuẩn
        },
    ];

   // console.debug("insertRow → sending rows:", JSON.stringify(rows));

    try {
        await table.insert(rows, { raw: true,skipInvalidRows: false, ignoreUnknownValues: false  });
        console.log(`✅ BigQuery inserted → ${tableName}`, { insertId: id });
    } catch (err) {
        if (err.name === "PartialFailureError") {
            err.errors?.forEach((e) => {
                console.error("❌ BigQuery partial error:", e.errors);
                console.error("➡️ Row:", e.row);
            });
        } else {
            console.error("❌ BigQuery insert error:", err);
        }
        throw err; // để function biết insert failed
    }
}
// Path lưu giống nhau nên gôm chung
export const realtimeToBigQuery = onValueCreated(
    "/Database/{tableName}/{userid}/{date}/{pushId}",
    async (event) => {

        const {pushId,tableName} = event.params;
        const data = event.data.val();

        const bqTable = tableNameMap[tableName];

        // todo duplicate insert, recheck

        const payload = {
            key: pushId,  // luôn ép = pushId, tránh xung đột
            ...data,
        };

        console.debug("Table->",bqTable,"Sessions inserting:", payload);
        await insertRow(bqTable, payload, pushId);

    }
);
/// Sesstion thêm 1 cấo nữa nên làm riêng
export const realtimeSession = onValueCreated(
    "/Database/Sessions/{date}/{userId}/{pushId}",
    async (event) => {
        const {pushId} = event.params;
        const data = event.data.val();

        const tableName = "sessions";

        const payload = {
            key: pushId,  // luôn ép = pushId, tránh xung đột
            ...data,
        };
        // debug: in giá trị insertId và payload trước khi insert
        console.debug("Sessions inserting:", payload);
        await insertRow(tableName, payload, pushId);

    }
);

export const realtimeLocationGuide = onValueCreated(
    "/Database/LocationGuide/{userId}/{date}/{pushId}",
    async (event) => {

        const {pushId} = event.params;
        const tableName ="location_guide";
        const data = event.data.val();

        const payload = {
            key: pushId,
            ...data,
        };
        console.debug("LocationGuide inserting:", payload);
        await insertRow(tableName, payload, pushId);
    }
);


export const realtimeDuplicate = onValueCreated(
    "/Database/TestDuplicate/{userId}/{date}/{pushId}",
    async (event) => {

        const {pushId} = event.params;
        const tableName ="test_duplicate";
        const data = event.data.val();


        const payload = {
            key: pushId,
            ...data,
        };
        // debug: in giá trị insertId và payload trước khi insert
        console.debug(":TestDuplicate inserting", { tableName, pushId, payload });
        await insertRow(tableName, payload, pushId);

    }
);

import {onValueCreated} from "firebase-functions/v2/database";
import {setGlobalOptions} from "firebase-functions/v2";
import {BigQuery} from "@google-cloud/bigquery";
import {tableNameMap} from "./constants.js";
//import {checkKeyExists} from "./validate/util.js";


const bigquery = new BigQuery();
setGlobalOptions({region: "us-central1", timeoutSeconds: 300});

const datasetId = "tracking";
//const fieldKey = "key";
async function insertRow(tableName, payload, insertId) {
    const table = bigquery.dataset(datasetId).table(tableName);

    const id = String(insertId || "");
    if (!id) console.warn("insertRow: insertId is falsy", { insertId });

    const rows = [
        {
            insertId: id,
            json: payload,
        },
    ];

    // debug log: cho thấy chính xác gì sẽ gửi
    console.debug("insertRow: rows:", JSON.stringify(rows));

    try {
        // raw:true rất quan trọng để client gửi đúng định dạng insertAll (insertId + json)
        await table.insert(rows, { raw: true });
        console.log("Inserted", { tableName, insertId: id });
    } catch (err) {
        if (err.name === "PartialFailureError") {
            err.errors?.forEach((e) => {
                console.error("BigQuery partial error:", e.errors);
                console.error("Row that failed:", e.row);
            });
        } else {
            console.error("BigQuery insert error:", err);
        }
        throw err;
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
        // debug: in giá trị insertId và payload trước khi insert
        console.debug(": inserting", { bqTable, pushId, payload });

        try {
            // dùng pushId làm insertId để đảm bảo idempotency
            await insertRow(bqTable, payload, pushId);
        } catch (err) {
            console.error("❌ Failed insert:", err);
            // tuỳ xử lý: nếu bạn không muốn function retry, không rethrow
            // nếu rethrow -> function có thể retry khiến duplicate nếu insertId không cố định
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

        const payload = {
            key: pushId,  // luôn ép = pushId, tránh xung đột
            ...data,
        };
        // debug: in giá trị insertId và payload trước khi insert
        console.debug(": inserting", { tableName, pushId, payload });

        try {
            // dùng pushId làm insertId để đảm bảo idempotency
            await insertRow(tableName, payload, pushId);
        } catch (err) {
             console.error("❌ Failed insert:", err);
            // tuỳ xử lý: nếu bạn không muốn function retry, không rethrow
            // nếu rethrow -> function có thể retry khiến duplicate nếu insertId không cố định
        }
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
        // debug: in giá trị insertId và payload trước khi insert
        console.debug(": inserting", { tableName, pushId, payload });

        try {
            // dùng pushId làm insertId để đảm bảo idempotency
            await insertRow(tableName, payload, pushId);
        } catch (err) {
            console.error("❌ Failed insert:", err);
            // tuỳ xử lý: nếu bạn không muốn function retry, không rethrow
            // nếu rethrow -> function có thể retry khiến duplicate nếu insertId không cố định
        }
    }
);


export const realtimeDuplicate = onValueCreated(
    "/Database/testDuplicate/{userId}/{date}/{pushId}",
    async (event) => {

        const {pushId} = event.params;
        const tableName ="test_duplicate";
        const data = event.data.val();


        const payload = {
            key: pushId,
            ...data,
        };
        // debug: in giá trị insertId và payload trước khi insert
        console.debug(": inserting", { tableName, pushId, payload });

        try {
            // dùng pushId làm insertId để đảm bảo idempotency
            await insertRow(tableName, payload, pushId);
        } catch (err) {
            console.error("❌ Failed insert:", err);
            // tuỳ xử lý: nếu bạn không muốn function retry, không rethrow
            // nếu rethrow -> function có thể retry khiến duplicate nếu insertId không cố định
        }
    }
);

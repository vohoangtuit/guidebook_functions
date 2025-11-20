import { onRequest } from "firebase-functions/v2/https";
import { onValueCreated } from "firebase-functions/v2/database";
import { setGlobalOptions } from "firebase-functions/v2";
import { BigQuery } from "@google-cloud/bigquery";
import { tableNameMap } from "./constants.js";
const bigquery = new BigQuery();
setGlobalOptions({ region: "us-central1", timeoutSeconds: 300 });

const datasetId = "tracking";


 export const insertToBigQuery = onRequest(async (req, res) => {
    const data = req.body;
    const { key, tableName, date } = data;
  
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
      const data = event.data.val();
      const key = event.data.key;
      const { tableName } = event.params;
        const bqTable = tableNameMap[tableName];
        console.log(`🔥 New realtimeToBigQuery Record: ${tableName}`, key);
      const row = {
        key,
        ...data
      };
     // console.log("📦 Payload gửi lên:", JSON.stringify(payload, null, 2));
      try {
        //await axios.post(urlInsert, payload);
          await bigquery.dataset(datasetId).table(bqTable).insert([row]);
       // console.log("📤 Dispatched to Cloud Task (Realtime)");
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
      const data = event.data.val();
      const key = event.data.key;

        const tableId = "sessions";
    //  const { date } = event.params;
        console.log("🔥 New Sessions Record:", key);
      const row = {
        key,
        ...data
      };
      try {
        //await axios.post(urlInsert, payload);
          await bigquery.dataset(datasetId).table(tableId).insert([row]);

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

/// LocationGuide thêm 1 cấo nữa nên làm riêng
export const realtimeLocationGuide = onValueCreated(
    "/Database/LocationGuide/{userId}/{date}/{pushId}",
    async (event) => {
        const data = event.data.val();
        const key = event.data.key;

        const tableId = "location_guide";// name from bigquery
     //   const { date } = event.params;
      //  console.log("🔥 New location Record:", key);
        const row = {
            key,
            ...data
        };

        try {
            await bigquery.dataset(datasetId).table(tableId).insert([row]);
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


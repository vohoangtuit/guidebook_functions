import {BigQuery} from "@google-cloud/bigquery";

const bigquery = new BigQuery();

export async function checkKeyExists(datasetId, tableName, keyField, keyValue) {
    const query = `
    SELECT 1 
    FROM \`${datasetId}.${tableName}\`
    WHERE ${keyField} = @key
    LIMIT 1
  `;

    try {
        const [rows] = await bigquery.query({
            query,
            params: { key: keyValue },
        });

        return rows.length > 0;  // true = tồn tại
    } catch (err) {
        console.error("❌ checkKeyExists SQL error:", err);
        return false;
    }
}
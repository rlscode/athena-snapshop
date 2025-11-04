import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryResultsCommand,
  GetQueryExecutionCommand
} from '@aws-sdk/client-athena';
import sql from 'mssql';

// ⚙️ Configuración de Athena
const athenaClient = new AthenaClient({
  region: 'us-east-1',
  credentials: {
    accessKeyId: '',
    secretAccessKey: ''
  }
});

// ⚙️ Configuración de SQL Server
const sqlConfig = {
  user: 'svc_softland',
  password: 'CKX90ek,dop3,cp@xc78yc5^6DZm*$844rcvf',
  database: 'TBSnapshots',
  server: '10.169.11.44',
  options: {
    encrypt: false,
    trustServerCertificate: true
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

// 🔹 1. Ejecutar consulta en Athena
async function fetchAllAthenaResults(query) {
  const start = await athenaClient.send(
    new StartQueryExecutionCommand({
      QueryString: query,
      QueryExecutionContext: { Database: 'ttb_dnicni_curated' },
      WorkGroup: 'ttb_dnicni_wg',
      ResultConfiguration: {
        OutputLocation:
          's3://timebilling-dw-curated-layer/products/ttb/dnicni/query_results/'
      }
    })
  );

  const queryExecutionId = start.QueryExecutionId;
  console.log('QueryExecutionId:', queryExecutionId);

  // Esperar hasta que termine
  let status = 'RUNNING';
  while (status === 'RUNNING' || status === 'QUEUED') {
    await new Promise((r) => setTimeout(r, 2000));
    const data = await athenaClient.send(
      new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId })
    );
    status = data.QueryExecution.Status.State;
  }

  if (status !== 'SUCCEEDED') throw new Error(`Query failed: ${status}`);

  // Obtener todos los resultados paginados
  let results = [];
  let nextToken = null;
  do {
    const response = await athenaClient.send(
      new GetQueryResultsCommand({
        QueryExecutionId: queryExecutionId,
        NextToken: nextToken
      })
    );

    const rows = response.ResultSet.Rows.map((r) =>
      r.Data.map((d) => d.VarCharValue || null)
    );
    results.push(...rows);
    nextToken = response.NextToken;
  } while (nextToken);

  console.log(`✅ Total rows fetched from Athena: ${results.length}`);
  return results;
}

// 🔹 2. Insertar masivamente en SQL Server
async function bulkInsertToSQL(rows) {
  if (rows.length <= 1) {
    console.log('No hay datos para insertar.');
    return;
  }

  const headers = rows[0];
  const dataRows = rows.slice(1);
  const snapshotDate = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

  const pool = await sql.connect(sqlConfig);

  // Obtener columnas válidas de la tabla destino
  const { recordset: schema } = await pool
    .request()
    .query(
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'time_entries_history'"
    );

  const validColumns = schema.map((r) => r.COLUMN_NAME.toLowerCase());
  const insertableColumns = headers
    .filter((h) => validColumns.includes(h.toLowerCase()))
    .concat('snapshot_date');

  console.log(`📋 Columnas insertables: ${insertableColumns.join(', ')}`);

  // Crear tabla en memoria para el bulk
  const table = new sql.Table('time_entries_history');
  table.create = false; // no crear la tabla real

  insertableColumns.forEach((col) => {
    table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
  });

  // Agregar filas con snapshot_date
  for (const row of dataRows) {
    const filtered = row.filter((_, i) =>
      validColumns.includes(headers[i].toLowerCase())
    );
    filtered.push(snapshotDate);
    table.rows.add(...filtered);
  }

  // Ejecutar bulk insert
  const request = new sql.Request(pool);
  await request.bulk(table);

  console.log(
    `🚀 Insertadas ${dataRows.length} filas en dbo.time_entries_history con snapshot_date = ${snapshotDate}`
  );
  await pool.close();
}

// 🔹 3. Ejecutar todo el flujo
(async () => {
  try {
    const query = 'SELECT * FROM time_entries_view';
    const rows = await fetchAllAthenaResults(query);
    await bulkInsertToSQL(rows);
  } catch (err) {
    console.error('❌ Error:', err);
  }
})();

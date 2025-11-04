import 'dotenv/config';
import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand
} from '@aws-sdk/client-athena';
import sql from 'mssql';
import nodemailer from 'nodemailer';
import cron from 'node-cron';

/* =========================
   1) Configuración
   ========================= */

// Athena
const athenaClient = new AthenaClient({
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID, // ⚠️ .env
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY // ⚠️ .env
  }
});

// SQL Server
const sqlConfig = {
  user: process.env.SQL_USER, // ⚠️ .env
  password: process.env.SQL_PASSWORD, // ⚠️ .env
  database: process.env.SQL_DATABASE || 'TBSnapshots',
  server: process.env.SQL_SERVER, // IP o FQDN, ⚠️ .env
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

// Nodemailer (Exchange)
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST || 'exchange.dentons.global',
  port: Number(process.env.SMTP_PORT || 25),
  secure: false,
  auth: {
    user: process.env.SMTP_USER, // 'LAC\\usuario'
    pass: process.env.SMTP_PASS
  },
  tls: { rejectUnauthorized: false }
});

// Parámetros generales
const ATHENA_DB = process.env.ATHENA_DB || 'ttb_dnicni_curated';
const ATHENA_WG = process.env.ATHENA_WG || 'ttb_dnicni_wg';
const ATHENA_OUTPUT =
  process.env.ATHENA_OUTPUT ||
  's3://timebilling-dw-curated-layer/products/ttb/dnicni/query_results/';

// Definir el mapeo de tipos para cada tabla
const columnTypeMappings = {
  prebills_history: {
    id: sql.BigInt,
    state: sql.NVarChar(sql.MAX),
    reviewed_at: sql.DateTime,
    issued_at: sql.DateTime,
    sent_at: sql.DateTime,
    invoiced_at: sql.DateTime,
    partial_payment_at: sql.DateTime,
    payment_at: sql.DateTime,
    from_date: sql.Date,
    to_date: sql.Date,
    client_code: sql.NVarChar(sql.MAX),
    amount: sql.Float, // Usar Float en lugar de Decimal
    agreement_id: sql.BigInt,
    rate_currency_id: sql.BigInt,
    amount_currency_id: sql.BigInt,
    base_currency_id: sql.BigInt,
    exchange_rate: sql.Float, // Usar Float en lugar de Decimal
    discount_type: sql.NVarChar(sql.MAX),
    discount_percentage: sql.Float, // Usar Float
    discount_amount: sql.Float, // Usar Float
    billing_strategy: sql.NVarChar(sql.MAX),
    user_id: sql.BigInt,
    responsible_user_id: sql.BigInt,
    secondary_responsible_user_id: sql.BigInt,
    total_minutes_billed: sql.Int,
    total_minutes_worked: sql.Int,
    honorarium_amount: sql.Float, // Usar Float
    honorarium_vat: sql.Float, // Usar Float
    honorarium_tax_percentage: sql.Float, // Usar Float
    expenses_amount: sql.Float, // Usar Float
    expenses_vat: sql.Float, // Usar Float
    expenses_tax_percentage: sql.Float, // Usar Float
    includes_honorarium: sql.Bit,
    includes_expenses: sql.Bit,
    billing_currency_id: sql.BigInt,
    created_at: sql.DateTime,
    updated_at: sql.DateTime,
    last_update_time: sql.DateTime,
    snapshot_date: sql.Date
  },
  time_entries_history: {
    id: sql.BigInt,
    project_code: sql.NVarChar(255),
    user_id: sql.BigInt,
    activity_name: sql.NVarChar(255),
    duration: sql.Float,
    formatted_duration: sql.NVarChar(50),
    billable_duration: sql.Float,
    formatted_billable_duration: sql.NVarChar(50),
    billable: sql.Bit,
    visible: sql.Bit,
    prebill_id: sql.BigInt,
    date: sql.Date,
    reviewed: sql.Bit,
    currency_id: sql.BigInt,
    billed_amount: sql.Float,
    description: sql.NVarChar(sql.MAX),
    hours_cost: sql.Float,
    hours_base_currency_cost: sql.Float,
    hours_rate: sql.Float,
    hours_standard_rate: sql.Float,
    billing_status: sql.NVarChar(100),
    created_at: sql.DateTime,
    updated_at: sql.DateTime,
    last_update_time: sql.DateTime,
    snapshot_date: sql.Date
  }
};

// Jobs que quieres correr como snapshots
// 🔁 Agrega aquí otros orígenes/destinos futuros si los necesitas
const JOBS = [
  {
    name: 'Time Entries',
    query: process.env.TIME_ENTRIES_QUERY || 'SELECT * FROM time_entries',
    destination: process.env.TIME_ENTRIES_TABLE || 'time_entries_history',
    fieldMappings: columnTypeMappings.time_entries_history
  },
  {
    name: 'Prebills',
    query: process.env.PREBILLS_QUERY || 'SELECT * FROM prebills',
    destination: process.env.PREBILLS_TABLE || 'prebills_history',
    fieldMappings: columnTypeMappings.prebills_history
  }
];

const tableTypeMappings = {
  prebills_history: {
    // BIGINT
    id: sql.BigInt,
    agreement_id: sql.BigInt,
    rate_currency_id: sql.BigInt,
    amount_currency_id: sql.BigInt,
    base_currency_id: sql.BigInt,
    user_id: sql.BigInt,
    responsible_user_id: sql.BigInt,
    secondary_responsible_user_id: sql.BigInt,
    billing_currency_id: sql.BigInt,

    // DECIMAL
    amount: sql.Decimal(15, 2),
    exchange_rate: sql.Decimal(15, 4),
    discount_percentage: sql.Decimal(10, 2),
    discount_amount: sql.Decimal(15, 2),
    honorarium_amount: sql.Decimal(15, 2),
    honorarium_vat: sql.Decimal(15, 2),
    honorarium_tax_percentage: sql.Decimal(10, 2),
    expenses_amount: sql.Decimal(15, 2),
    expenses_vat: sql.Decimal(15, 2),
    expenses_tax_percentage: sql.Decimal(10, 2),

    // INT
    total_minutes_billed: sql.Int,
    total_minutes_worked: sql.Int,

    // BIT
    includes_honorarium: sql.Bit,
    includes_expenses: sql.Bit,

    // DATE
    from_date: sql.Date,
    to_date: sql.Date,
    snapshot_date: sql.Date,

    // DATETIME
    reviewed_at: sql.DateTime,
    issued_at: sql.DateTime,
    sent_at: sql.DateTime,
    invoiced_at: sql.DateTime,
    partial_payment_at: sql.DateTime,
    payment_at: sql.DateTime,
    created_at: sql.DateTime,
    updated_at: sql.DateTime,
    last_update_time: sql.DateTime
  },

  time_entries_history: {
    // Ejemplo para otra tabla
    id: sql.BigInt,
    user_id: sql.BigInt,
    minutes: sql.Int,
    billable: sql.Bit,
    date: sql.Date,
    created_at: sql.DateTime,
    updated_at: sql.DateTime
  }

  // Puedes agregar más tablas aquí...
};

// Función para convertir valores según el tipo
function convertValue(value, columnName, columnType) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  // Manejar fechas placeholder
  if (typeof value === 'string' && value === '1/1/1900') {
    return null;
  }

  // CONVERSIÓN EXPLÍCITA PARA FECHAS
  if (columnType === sql.Date || columnType === sql.DateTime) {
    if (typeof value === 'string') {
      try {
        // Para strings como "2024-07-26 19:56:31.000000"
        // Reemplazar espacio por 'T' para crear Date válido
        const dateString = value.replace(' ', 'T');
        const date = new Date(dateString);

        // Verificar que sea una fecha válida
        if (isNaN(date.getTime())) {
          console.warn(
            `⚠️ Fecha inválida: ${value} para columna ${columnName}`
          );
          return null;
        }

        return date;
      } catch (error) {
        console.warn(`⚠️ Error convirtiendo fecha: ${value}`, error);
        return null;
      }
    }
  }

  // Resto de conversiones...
  if (columnType === sql.Bit) {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
      const lowerVal = value.toLowerCase();
      return lowerVal === 'true' || lowerVal === '1' || lowerVal === 'yes';
    }
    return Boolean(value);
  }

  if (columnType === sql.BigInt || columnType === sql.Int) {
    const num = parseInt(value);
    return isNaN(num) ? null : num;
  }

  if (columnType === sql.Float) {
    const num = parseFloat(value);
    return isNaN(num) ? null : num;
  }

  return value;
}

// Programación (elige una con variable de entorno)
const SCHEDULE_MODE = (process.env.SCHEDULE_MODE || 'daily').toLowerCase();
// Diario: 08:00 / Mensual: día 1 a las 03:00
const CRON_EXPRESSIONS = {
  daily: '0 6 * * *',
  monthly: '0 3 1 * *'
};

const CRON_EXPR = CRON_EXPRESSIONS[SCHEDULE_MODE] || CRON_EXPRESSIONS.daily;

// Notificación
const NOTIFY_FROM = process.env.NOTIFY_FROM; // 'doc.electronicoscr@tudominio.com'
const NOTIFY_TO = (process.env.NOTIFY_TO || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

/* =========================
   2) Utilidades
   ========================= */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const todayISO = () => new Date().toISOString().slice(0, 10); // YYYY-MM-DD

function assertWhitelistedTable(tbl) {
  // Evita SQL Injection en identificadores (whitelist)
  const allowed = new Set(JOBS.map((j) => j.destination.toLowerCase()));
  if (!allowed.has(tbl.toLowerCase())) {
    throw new Error(`Tabla destino no permitida: ${tbl}`);
  }
}

function sqlQuoteIdent(name) {
  // En SQL Server, escapamos ] como ]]
  return `[${name.replaceAll(']', ']]')}]`;
}

/* =========================
   3) Athena
   ========================= */

async function fetchAllAthenaResults(query) {
  const start = await athenaClient.send(
    new StartQueryExecutionCommand({
      QueryString: query,
      QueryExecutionContext: { Database: ATHENA_DB },
      WorkGroup: ATHENA_WG,
      ResultConfiguration: { OutputLocation: ATHENA_OUTPUT }
    })
  );

  const queryExecutionId = start.QueryExecutionId;
  let state = 'RUNNING';

  // Poll con backoff suave
  let backoff = 1500;
  while (state === 'RUNNING' || state === 'QUEUED') {
    await sleep(backoff);
    const meta = await athenaClient.send(
      new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId })
    );
    state = meta.QueryExecution.Status.State;
    if (backoff < 8000) backoff += 500;
  }

  if (state !== 'SUCCEEDED') {
    throw new Error(`Athena query failed: ${state}`);
  }

  // Obtener todas las páginas
  const all = [];
  let nextToken = undefined;
  do {
    const page = await athenaClient.send(
      new GetQueryResultsCommand({
        QueryExecutionId: queryExecutionId,
        NextToken: nextToken
      })
    );
    const rows = (page.ResultSet?.Rows || []).map((r) =>
      (r.Data || []).map((d) => d.VarCharValue ?? null)
    );
    all.push(...rows);
    nextToken = page.NextToken;
  } while (nextToken);

  return all; // [ [col1, col2, ...], ... ] con encabezados en [0]
}

/* =========================
   4) SQL Server helpers
   ========================= */

async function ensureTableWithColumns(pool, tableName, headers) {
  // Crea la tabla si no existe y agrega columnas faltantes como NVARCHAR(MAX)
  const tableQuoted = tableName; // conservamos para IDENTIFICADOR en otras funciones
  const req = pool.request();

  const { recordset: tables } = await req.query(`
    SELECT 1 AS exists_flag
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '${tableName}'
  `);

  if (tables.length === 0) {
    // Crear tabla desde cero
    const cols = headers
      .map((h) => `${sqlQuoteIdent(h)} NVARCHAR(MAX) NULL`)
      .join(',\n  ');
    const create = `
      CREATE TABLE dbo.${sqlQuoteIdent(tableName)} (
        ${cols},
        ${sqlQuoteIdent('snapshot_date')} DATE NOT NULL
      );
    `;
    await pool.request().query(create);
  } else {
    // Agregar columnas nuevas si hiciera falta
    const { recordset: schema } = await pool.request().query(`
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '${tableName}'
    `);
    const existing = new Set(schema.map((r) => r.COLUMN_NAME.toLowerCase()));

    const toAdd = headers.filter((h) => !existing.has(h.toLowerCase()));
    for (const col of toAdd) {
      const alter = `
        ALTER TABLE dbo.${sqlQuoteIdent(tableName)}
        ADD ${sqlQuoteIdent(col)} NVARCHAR(MAX) NULL;
      `;
      await pool.request().query(alter);
    }
    if (!existing.has('snapshot_date')) {
      await pool.request().query(`
        ALTER TABLE dbo.${sqlQuoteIdent(tableName)}
        ADD ${sqlQuoteIdent('snapshot_date')} DATE NOT NULL;
      `);
    }
  }
}

// async function bulkInsertToSQL(rows, tableName) {
//   assertWhitelistedTable(tableName);

//   if (!rows || rows.length <= 1) {
//     return { inserted: 0, skipped: true, reason: 'Sin datos' };
//   }

//   const headers = rows[0].map((h) => (h ?? '').toString().trim());
//   const dataRows = rows.slice(1);
//   const snapshotDate = todayISO();

//   const pool = await sql.connect(sqlConfig);
//   try {
//     // Asegurar existencia y columnas
//     await ensureTableWithColumns(pool, tableName, headers);

//     // Tomar columnas válidas de la tabla destino
//     const { recordset: schema } = await pool.request().query(`
//       SELECT COLUMN_NAME
//       FROM INFORMATION_SCHEMA.COLUMNS
//       WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '${tableName}'
//     `);
//     const valid = new Set(schema.map((r) => r.COLUMN_NAME.toLowerCase()));
//     const insertable = headers
//       .filter((h) => valid.has(h.toLowerCase()))
//       .concat('snapshot_date');

//     // Evitar duplicado del día (borrado por snapshot_date)
//     await pool
//       .request()
//       .input('snap', sql.Date, snapshotDate)
//       .query(
//         `DELETE FROM dbo.${sqlQuoteIdent(tableName)} WHERE ${sqlQuoteIdent(
//           'snapshot_date'
//         )} = @snap;`
//       );

//     // Preparar tabla en memoria para bulk
//     const t = new sql.Table(`dbo.${tableName}`);
//     t.create = false;
//     for (const col of insertable) {
//       // NVARCHAR(MAX) por simplicidad; si deseas mapear tipos, se puede extender
//       t.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
//     }

//     for (const row of dataRows) {
//       const filtered = [];
//       for (let i = 0; i < headers.length; i++) {
//         if (valid.has(headers[i].toLowerCase())) {
//           filtered.push(row[i] ?? null);
//         }
//       }
//       filtered.push(snapshotDate);
//       t.rows.add(...filtered);
//     }

//     const req = new sql.Request(pool);
//     await req.bulk(t);

//     return { inserted: dataRows.length, table: tableName, snapshotDate };
//   } finally {
//     await pool.close();
//   }
// }

// async function bulkInsertToSQL(rows, tableName) {
//   if (!rows || rows.length <= 1) {
//     return { inserted: 0, skipped: true, reason: 'Sin datos' };
//   }

//   const headers = rows[0].map((h) => (h ?? '').toString().trim());
//   const dataRows = rows.slice(1);

//   // 🔹 Fecha del snapshot (tipo Date real)
//   const snapshotDate = new Date();

//   const pool = await sql.connect(sqlConfig);
//   try {
//     // 🔹 1. Borrar snapshots previos del mismo día
//     await pool
//       .request()
//       .input('snap', sql.Date, snapshotDate)
//       .query(`DELETE FROM dbo.${tableName} WHERE snapshot_date = @snap;`);

//     // 🔹 2. Crear tabla en memoria (sin crearla en SQL)
//     const table = new sql.Table(`dbo.${tableName}`);
//     table.create = false;

//     // 🔹 3. Agregar todas las columnas como NVARCHAR (estables y tolerantes)
//     for (const col of headers) {
//       table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
//     }

//     // 🔹 4. Agregar la columna snapshot_date como tipo DATE
//     table.columns.add('snapshot_date', sql.Date, { nullable: false });

//     // 🔹 5. Cargar filas con snapshot_date al final
//     for (const row of dataRows) {
//       const values = row.map((v) => (v == null ? null : v.toString().trim()));
//       values.push(snapshotDate);
//       table.rows.add(...values);
//     }

//     // 🔹 6. Ejecutar bulk insert
//     const request = new sql.Request(pool);
//     await request.bulk(table);

//     return {
//       inserted: dataRows.length,
//       table: tableName,
//       snapshotDate: snapshotDate.toISOString().split('T')[0]
//     };
//   } catch (err) {
//     console.error(`❌ Error bulkInsertToSQL(${tableName}):`, err);
//     throw err;
//   } finally {
//     await pool.close();
//   }
// }

// 🔹 2. Insertar masivamente en SQL Server
async function bulkInsertToSQL(rows, tableName, customMappings = {}) {
  // console.log('🚀 ~ bulkInsertToSQL ~ tableName:', tableName);
  // console.log('🚀 ~ bulkInsertToSQL ~ rows:', rows);
  if (rows.length <= 1) {
    console.log('No hay datos para insertar.');
    return;
  }

  const headers = rows[0];
  // console.log('🚀 ~ bulkInsertToSQL ~ headers:', headers);
  const dataRows = rows.slice(1);
  const snapshotDate = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

  const pool = await sql.connect(sqlConfig);

  try {
    // Obtener columnas válidas de la tabla destino
    const { recordset: schema } = await pool
      .request()
      .query(
        `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '${tableName}'`
      );

    const validColumns = schema.map((r) => r.COLUMN_NAME.toLowerCase());
    const insertableColumns = headers
      .filter((h) => validColumns.includes(h.toLowerCase()))
      .concat('snapshot_date');

    /* Avoid duplicate by snapshot_date */
    await pool
      .request()
      .input('snap', sql.Date, snapshotDate)
      .query(
        `DELETE FROM dbo.${sqlQuoteIdent(tableName)} WHERE ${sqlQuoteIdent(
          'snapshot_date'
        )} = @snap;`
      );

    // console.log(`📋 Columnas insertables: ${insertableColumns.join(', ')}`);

    // Combinar mapeos: customMappings tiene prioridad sobre tableTypeMappings
    // const defaultMappings = tableTypeMappings[tableName] || {};
    // const finalMappings = { ...defaultMappings, ...customMappings };

    // Crear tabla en memoria para el bulk
    const table = new sql.Table(tableName);

    table.create = false; // no crear la tabla real
    table.database = 'TBSnapshots';

    // Agregar columnas con tipos específicos
    insertableColumns.forEach((col) => {
      const columnType = customMappings[col];
      // console.log('🚀 ~ bulkInsertToSQL ~ col:', col);
      // console.log('🚀 ~ bulkInsertToSQL ~ columnType:', columnType);

      if (columnType) {
        table.columns.add(col, columnType, { nullable: true });
      }
      // } else {
      //   // Por defecto NVARCHAR para campos sin mapeo específico
      //   table.columns.add(col, sql.NVarChar(sql.MAX), { nullable: true });
      // }
    });

    // Agregar filas con snapshot_date
    for (const row of dataRows) {
      const filteredRow = [];

      // Filtrar y convertir cada valor según su columna
      headers.forEach((header, index) => {
        if (validColumns.includes(header.toLowerCase())) {
          const convertedValue = convertValue(
            row[index],
            header,
            customMappings[header]
          );
          filteredRow.push(convertedValue);
        }
      });

      // Agregar snapshot_date (convertido a Date)
      filteredRow.push(new Date(snapshotDate));

      table.rows.add(...filteredRow);
    }

    // Ejecutar bulk insert
    const request = new sql.Request(pool);
    // console.log('🚀 ~ bulkInsertToSQL ~ table:', table);
    // console.log(`📊 Insertando ${dataRows.length} filas en ${tableName}`);
    await request.bulk(table);

    console.log(
      `🚀 Insertadas ${dataRows.length} filas en dbo.time_entries_history con snapshot_date = ${snapshotDate}`
    );
    return { inserted: dataRows.length, table: tableName, snapshotDate };
  } catch (err) {
    console.error(`❌ Error bulkInsertToSQL(${tableName}):`, err);
    throw err;
  } finally {
    await pool.close();
  }
}

/* =========================
   5) Notificaciones por correo
   ========================= */

async function sendSummaryEmail({ startedAt, finishedAt, results, errors }) {
  if (!NOTIFY_FROM || NOTIFY_TO.length === 0) {
    console.warn(
      '⚠️ NOTIFY_FROM o NOTIFY_TO no configurados; no se enviará correo.'
    );
    return;
  }

  const okLines = results.map(
    (r) =>
      `✅ ${r.name}: ${r.inserted} filas en ${r.table} (snapshot ${r.snapshotDate})`
  );

  const errLines = errors.map(
    (e) => `❌ ${e.name}: ${e.error?.message || e.error}`
  );

  const html = `
    <h2>Reporte de snapshots</h2>
    <p><b>Inicio:</b> ${startedAt.toISOString()}<br/>
       <b>Fin:</b> ${finishedAt.toISOString()}</p>
    <h3>Resultados</h3>
    <ul>${okLines.map((l) => `<li>${l}</li>`).join('')}</ul>
    ${
      errLines.length
        ? `<h3>Errores</h3><ul>${errLines
            .map((l) => `<li>${l}</li>`)
            .join('')}</ul>`
        : '<p>Sin errores.</p>'
    }
  `;

  const subject = `[Snapshots] ${todayISO()} — ${results.length} OK ${
    errors.length ? `/ ${errors.length} errores` : ''
  }`;

  await transporter.sendMail({
    from: NOTIFY_FROM,
    to: NOTIFY_TO,
    subject,
    html
  });
}

/* =========================
   6) Runner y scheduler
   ========================= */

async function runSnapshot() {
  const startedAt = new Date();
  const ok = [];
  const errs = [];

  for (const job of JOBS) {
    try {
      console.log(`▶️  ${job.name}: ejecutando Athena...`);
      const rows = await fetchAllAthenaResults(job.query);
      console.log(`Athena devolvió ${rows.length} filas (${job.name})`);

      console.log(`▶️  ${job.name}: insertando en SQL (${job.destination})...`);
      const res = await bulkInsertToSQL(
        rows,
        job.destination,
        job.fieldMappings
      );
      ok.push({ name: job.name, ...res });
      console.log(
        `   ✔️ ${job.name}: ${res.inserted} filas insertadas en ${res.table}`
      );
    } catch (error) {
      console.error(`   ❌ ${job.name}:`, error);
      errs.push({ name: job.name, error });
    }
  }

  const finishedAt = new Date();
  console.log('🚀 ~ runSnapshot ~ finishedAt:', finishedAt);
  await sendSummaryEmail({ startedAt, finishedAt, results: ok, errors: errs });

  // Si quieres que el proceso permanezca vivo en modo scheduler, no hagas process.exit aquí
  // En modo "ejecución única", podrías salir con código según errores:
  if (process.env.ONE_SHOT === 'true') {
    process.exit(errs.length ? 1 : 0);
  }

  // Si estás ejecutando bajo PM2 y quieres que PM2 reinicie el proceso cuando haya errores,
  // activa RESTART_ON_ERROR=true en tu entorno/archivo de configuración de PM2.
  if (process.env.RESTART_ON_ERROR === 'true' && errs.length > 0) {
    console.error(
      'Errores detectados → saliendo para que PM2 reinicie el proceso.'
    );
    // Dar tiempo para flush de logs/notificaciones antes de salir
    setTimeout(() => process.exit(1), 3000);
  }
}

// Ejecutar una vez al inicio si quieres (útil para pruebas)
if (process.env.RUN_ON_START === 'true') {
  console.log('🚀 Athena Snapshot Service iniciado y conectado a SQL Server.');
  runSnapshot().catch((err) => console.error('Error en runSnapshot:', err));
}

// Scheduler
cron.schedule(
  CRON_EXPR,
  () => {
    console.log(
      `⏰ Disparador (${SCHEDULE_MODE}) → ${new Date().toISOString()}`
    );
    runSnapshot().catch((err) =>
      console.error('Error en runSnapshot (cron):', err)
    );
  },
  {
    timezone: process.env.TZ || 'America/Costa_Rica'
  }
);

console.log('RUN_ON_START value:', process.env.RUN_ON_START);
console.log('RUN_ON_START value:', process.env.RUN_ON_START);
console.log(
  `🗓️ Scheduler activo (${SCHEDULE_MODE}) con expresión "${CRON_EXPR}".`
);

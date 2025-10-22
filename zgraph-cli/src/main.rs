use std::path::Path;

use clap::{Parser, Subcommand, ValueEnum};
use zcore_catalog as zcat;
use zcore_storage as zs;
use zann_hnsw as zann;
use zgraph::graph::NodeRef;
use zgraph::schema::TableSchema;
use zgraph::types::{Row, Value};

#[derive(Parser, Debug)]
#[command(name = "zgraph")] 
#[command(about = "Super simple relational+vector+relations DB", long_about = None)]
struct Cli {
    /// Database path (fjall directory). Created if missing.
    #[arg(long, global = true, default_value = "data.fjall")]
    db: String,
    /// Acting user id
    #[arg(long, global = true, default_value = "admin")]
    user: String,
    /// Organization id
    #[arg(long, global = true, default_value_t = 1)]
    org: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    CreateTable {
        #[arg(long)]
        name: String,
        #[arg(long)]
        vector_col: Option<String>,
        #[arg(long)]
        dims: Option<usize>,
        #[arg(long, value_enum, default_value_t = MetricArg::L2)]
        metric: MetricArg,
    },
    AddRole {
        #[arg(long)]
        user: String,
        #[arg(long)]
        role: String,
    },
    GrantTable {
        #[arg(long)]
        table: String,
        /// user:alice or role:reader
        #[arg(long)]
        subject: String,
        #[arg(long, value_enum)]
        perm: PermArg,
    },
    Insert {
        #[arg(long)]
        table: String,
        /// Row data as JSON, e.g. '{"title":"doc","embedding":[0.1,0.2]}'
        #[arg(long)]
        json: String,
    },
    Select {
        #[arg(long)]
        table: String,
        /// Filter equality like field=value (may repeat)
        #[arg(long)]
        filter: Vec<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    Knn {
        #[arg(long)]
        table: String,
        /// Query vector JSON array, e.g. '[0.1,0.2]'
        #[arg(long)]
        vec: String,
        #[arg(short, long, default_value_t = 5)]
        k: usize,
    },
    AddEdge {
        #[arg(long)]
        from_table: String,
        #[arg(long)]
        from_id: u64,
        #[arg(long)]
        rel: String,
        #[arg(long)]
        to_table: String,
        #[arg(long)]
        to_id: u64,
        #[arg(long, default_value = "{}")]
        props: String,
    },
    Neighbors {
        #[arg(long)]
        table: String,
        #[arg(long)]
        id: u64,
        #[arg(long)]
        rel: Option<String>,
    },
    Cypher {
        /// Cypher-like query string (subset)
        #[arg(long)]
        query: String,
    },
    FjCreateTable {
        #[arg(long)]
        db: String,
        #[arg(long, default_value_t = 1)]
        org: u64,
        #[arg(long)]
        table: String,
        #[arg(long)]
        vector_col: String,
    },
    FjInsert {
        #[arg(long)]
        db: String,
        #[arg(long, default_value_t = 1)]
        org: u64,
        #[arg(long)]
        table: String,
        /// Row JSON like '{"title":"a","embedding":[0,0,0]}'
        #[arg(long)]
        json: String,
    },
    FjBuildHnsw {
        #[arg(long)]
        db: String,
        #[arg(long, default_value_t = 1)]
        org: u64,
        #[arg(long)]
        table: String,
        #[arg(long)]
        column: String,
        #[arg(long, value_enum, default_value_t = MetricArg::L2)]
        metric: MetricArg,
        #[arg(long, default_value_t = 16)]
        m: usize,
        #[arg(long, default_value_t = 64)]
        efc: usize,
    },
    FjSearch {
        #[arg(long)]
        db: String,
        #[arg(long, default_value_t = 1)]
        org: u64,
        #[arg(long)]
        table: String,
        #[arg(long)]
        column: String,
        #[arg(long)]
        vec: String,
        #[arg(short, long, default_value_t = 5)]
        k: usize,
        #[arg(long, default_value_t = 64)]
        ef: usize,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum MetricArg {
    L2,
    Cosine,
    Dot,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum PermArg {
    Read,
    Write,
    Delete,
    Admin,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let user = cli.user.clone();
    let fdb = zgraph::fjall_db::FjDatabase::open(&cli.db, cli.org)?;

    use Cmd::*;
    match cli.cmd {
        CreateTable {
            name,
            vector_col,
            dims,
            metric,
        } => {
            let mut schema = TableSchema::new(&name);
            if let Some(vc) = vector_col {
                schema = schema.with_vector(vc, dims, match metric {
                    MetricArg::L2 => zgraph::index::Metric::L2,
                    MetricArg::Cosine => zgraph::index::Metric::Cosine,
                    MetricArg::Dot => zgraph::index::Metric::Dot,
                });
            }
            fdb.create_table(&name, schema)?;
            println!("created table: {}", name);
        }
        AddRole { user, role } => {
            fdb.add_role(&user, &role);
            println!("added role '{}' to user '{}'", role, user);
        }
        GrantTable {
            table,
            subject,
            perm,
        } => {
            let subject = parse_subject(&subject)?;
            let perm = match perm {
                PermArg::Read => zgraph::permissions::Permission::Read,
                PermArg::Write => zgraph::permissions::Permission::Write,
                PermArg::Delete => zgraph::permissions::Permission::Delete,
                PermArg::Admin => zgraph::permissions::Permission::Admin,
            };
            fdb.grant_table_permission(&table, subject, perm)?;
            println!("granted {:?} on {}", perm, table);
        }
        Insert { table, json } => {
            let v: serde_json::Value = serde_json::from_str(&json)?;
            let row = to_row(v);
            let id = fdb.insert_row(&user, &table, row)?;
            println!("inserted id={}", id);
        }
        Select { table, filter, limit } => {
            let filters = parse_filters(filter);
            let mut rows = fdb.select_where(&user, &table, |r| apply_filters(r, &filters))?;
            if let Some(l) = limit {
                rows.truncate(l);
            }
            for (id, row) in rows {
                println!("{} => {}", id, serde_json::to_string(&row)?);
            }
        }
        Knn { table, vec, k } => {
            let qv: Vec<f32> = serde_json::from_str(&vec)?;
            let res = fdb.knn(&user, &table, &qv, k)?;
            for (id, d) in res {
                println!("{} dist={:.6}", id, d);
            }
        }
        AddEdge {
            from_table,
            from_id,
            rel,
            to_table,
            to_id,
            props,
        } => {
            let props_v: serde_json::Value = serde_json::from_str(&props)?;
            let props_row = to_row(props_v);
            let eid = fdb.add_edge(
                &user,
                NodeRef {
                    table: from_table,
                    id: from_id,
                },
                &rel,
                NodeRef {
                    table: to_table,
                    id: to_id,
                },
                props_row,
            )?;
            println!("edge id={}", eid);
        }
        Neighbors { table, id, rel } => {
            let res = fdb.neighbors(&user, &NodeRef { table, id }, rel.as_deref())?;
            for (eid, n) in res {
                println!("edge {} -> {}:{}", eid, n.table, n.id);
            }
        }
        Cypher { query } => {
            let rows = fdb.cypher(&user, &query)?;
            for r in rows {
                let mut printable: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
                for (k, (nref, row)) in r.vars.iter() {
                    let mut obj = serde_json::Map::new();
                    obj.insert("table".to_string(), serde_json::Value::String(nref.table.clone()));
                    obj.insert("id".to_string(), serde_json::Value::Number((*nref.id as u64).into()));
                    obj.insert("row".to_string(), serde_json::to_value(row).unwrap_or(serde_json::Value::Null));
                    printable.insert(k.clone(), serde_json::Value::Object(obj));
                }
                if let Some(d) = r.distance {
                    printable.insert("distance".to_string(), serde_json::json!(d));
                }
                println!("{}", serde_json::Value::Object(printable));
            }
        }
        FjCreateTable { db, org, table, vector_col } => {
            let store = zs::Store::open(&db)?;
            let catalog = zcat::Catalog::new(&store);
            let def = zcat::TableDef::new(
                table.clone(),
                vec![
                    zcat::ColumnDef::primary_key("id".to_string(), zcat::ColumnType::Integer),
                    zcat::ColumnDef::not_null(vector_col.clone(), zcat::ColumnType::Vector),
                ],
            );
            catalog.create_table(org, &def)?;
            println!("fjall: created table {} with vector {}", table, vector_col);
        }
        FjInsert { db, org, table, json } => {
            let store = zs::Store::open(&db)?;
            let catalog = zcat::Catalog::new(&store);
            let tdef = catalog.get_table(org, &table)?.ok_or_else(|| anyhow::anyhow!("no table"))?;
            let v: serde_json::Value = serde_json::from_str(&json)?;
            let cells = row_to_cells(&tdef, &v)?;
            let mut w = zs::WriteTransaction::new();
            let row_id = next_seq(&store, &table)?;
            let key = zs::encode_key(&[b"rows", &org.to_be_bytes(), table.as_bytes(), &row_id.to_be_bytes()]);
            let bytes = bincode::serialize(&cells)?;
            w.set(zs::COL_ROWS, key, bytes);
            w.commit(&store)?;
            println!("fjall: inserted id={}", row_id);
        }
        FjBuildHnsw { db, org, table, column, metric, m, efc } => {
            let store = zs::Store::open(&db)?;
            let catalog = zcat::Catalog::new(&store);
            let metric = match metric { MetricArg::L2 => zann::Metric::L2, MetricArg::Cosine => zann::Metric::Cosine, MetricArg::Dot => zann::Metric::InnerProduct };
            zann::build_index(&store, &catalog, org, &table, &column, metric, m, efc)?;
            println!("fjall: built HNSW for {}.{} m={} efc={}", table, column, m, efc);
        }
        FjSearch { db, org, table, column, vec, k, ef } => {
            let store = zs::Store::open(&db)?;
            let catalog = zcat::Catalog::new(&store);
            let qv: Vec<f32> = serde_json::from_str(&vec)?;
            let res = zann::search(&store, &catalog, org, &table, &column, &qv, k, ef)?;
            for (id, d) in res { println!("{} dist={:.6}", id, d); }
        }
    }

    // Save after mutating commands
    match &cli.cmd {
        Cmd::Select { .. } | Cmd::Knn { .. } | Cmd::Neighbors { .. } => {}
        _ => { /* fjall-backed ops are durable */ }
    }

    Ok(())
}

// removed old JSON DB loader; using fjall via FjDatabase

fn to_row(v: serde_json::Value) -> Row {
    match v {
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(k, v)| (k, to_value(v)))
            .collect(),
        _ => Row::default(),
    }
}

fn to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Json(serde_json::Value::Number(n))
            }
        }
        serde_json::Value::String(s) => Value::Text(s),
        serde_json::Value::Array(arr) => {
            // Heuristic: treat array of numbers as Vector; else JSON
            if arr.iter().all(|e| e.is_number()) {
                let mut v = Vec::with_capacity(arr.len());
                for e in arr {
                    let f = e.as_f64().unwrap_or(0.0) as f32;
                    v.push(f);
                }
                Value::Vector(v)
            } else {
                Value::Json(serde_json::Value::Array(arr))
            }
        }
        serde_json::Value::Object(_) => Value::Json(v),
    }
}

fn parse_subject(s: &str) -> anyhow::Result<zgraph::permissions::Subject> {
    let (kind, rest) = s
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid subject '{}', use user:... or role:...", s))?;
    Ok(match kind {
        "user" => zgraph::permissions::Subject::User(rest.to_string()),
        "role" => zgraph::permissions::Subject::Role(rest.to_string()),
        _ => anyhow::bail!("unknown subject kind '{}'", kind),
    })
}

type Filter = (String, Value);

fn parse_filters(filters: Vec<String>) -> Vec<Filter> {
    let mut out = Vec::new();
    for f in filters {
        if let Some((k, v)) = f.split_once('=') {
            // Try parse as number/bool; else string
            if let Ok(i) = v.parse::<i64>() {
                out.push((k.to_string(), Value::Int(i)));
            } else if let Ok(fv) = v.parse::<f64>() {
                out.push((k.to_string(), Value::Float(fv)));
            } else if let Ok(b) = v.parse::<bool>() {
                out.push((k.to_string(), Value::Bool(b)));
            } else {
                out.push((k.to_string(), Value::Text(v.to_string())));
            }
        }
    }
    out
}

fn apply_filters(row: &Row, filters: &Vec<Filter>) -> bool {
    for (k, v) in filters.iter() {
        match (row.get(k), v) {
            (Some(Value::Int(a)), Value::Int(b)) if a == b => {}
            (Some(Value::Float(a)), Value::Float(b)) if (*a - *b).abs() < 1e-9 => {}
            (Some(Value::Bool(a)), Value::Bool(b)) if a == b => {}
            (Some(Value::Text(a)), Value::Text(b)) if a == b => {}
            // JSON and Vector equality matching not supported in this simple filter
            _ => return false,
        }
    }
    true
}

// --- Fjall helpers ---
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Cell {
    Int(i64),
    Float(f64),
    Text(String),
    Vector(Vec<f32>),
}

fn row_to_cells(tdef: &zcat::TableDef, v: &serde_json::Value) -> anyhow::Result<Vec<Cell>> {
    let obj = v.as_object().ok_or_else(|| anyhow::anyhow!("row must be object"))?;
    let mut cells = Vec::with_capacity(tdef.columns.len());
    for col in &tdef.columns {
        let val = obj.get(&col.name);
        let cell = match col.ty {
            zcat::ColumnType::Integer => Cell::Int(val.and_then(|x| x.as_i64()).unwrap_or_default()),
            zcat::ColumnType::Float => Cell::Float(val.and_then(|x| x.as_f64()).unwrap_or_default()),
            zcat::ColumnType::Text => Cell::Text(val.and_then(|x| x.as_str()).unwrap_or_default().to_string()),
            zcat::ColumnType::Vector => {
                let arr = val.and_then(|x| x.as_array()).ok_or_else(|| anyhow::anyhow!("vector column '{}' requires array", col.name))?;
                let vec: Vec<f32> = arr.iter().map(|e| e.as_f64().unwrap_or(0.0) as f32).collect();
                Cell::Vector(vec)
            }
            zcat::ColumnType::Decimal | zcat::ColumnType::Int | zcat::ColumnType::Integer => {
                // Treat as integer by default
                Cell::Int(val.and_then(|x| x.as_i64()).unwrap_or_default())
            }
        };
        cells.push(cell);
    }
    Ok(cells)
}

fn next_seq(store: &zs::Store, table: &str) -> anyhow::Result<u64> {
    Ok(zs::next_seq(store, table.as_bytes())?)
}

use super::helpers;

use datanymizer_dumper::{
    postgres::{dumper::PgDumper, writer::DumpWriter},
    Dumper,
};
use datanymizer_engine::{Engine, Settings};

fn dump(name: &str) {
    let mut dst = helpers::dst_wrapper(name);

    let cfg_filename = format!("tests/postgres/configs/{}.yml", name);
    let settings = Settings::new(cfg_filename, helpers::src_database_url().into_string()).unwrap();
    let engine = Engine::new(settings);
    let writer = DumpWriter::for_child(dst.io()).unwrap();
    let mut dumper = PgDumper::new(engine, None, helpers::pg_dump_path(), writer, vec![]).unwrap();
    dumper.dump(&mut helpers::src_connection()).unwrap();

    dst.close();
}

#[test]
fn simple_dump() {
    dump("simple");

    let mut src_connection = helpers::src_connection();
    let mut dst_connection = helpers::dst_connection("simple");

    let count_query = "SELECT COUNT(*) FROM actor";
    let src_count: i64 = src_connection.query_one(count_query, &[]).unwrap().get(0);
    let dst_count: i64 = dst_connection.query_one(count_query, &[]).unwrap().get(0);
    assert_eq!(src_count, dst_count);

    let rows_query = "SELECT * FROM actor";
    let src_rows = src_connection.query(rows_query, &[]).unwrap();
    let dst_rows = dst_connection.query(rows_query, &[]).unwrap();
    for (i, src_row) in src_rows.iter().enumerate() {
        let dst_row = &dst_rows[i];

        let src_actor_id: i32 = src_row.get("actor_id");
        let dst_actor_id: i32 = dst_row.get("actor_id");
        assert_eq!(src_actor_id, dst_actor_id);

        let src_first_name: String = src_row.get("first_name");
        let dst_first_name: String = dst_row.get("first_name");
        assert_ne!(src_first_name, dst_first_name);

        let src_last_name: String = src_row.get("last_name");
        let dst_last_name: String = dst_row.get("last_name");
        assert_ne!(src_last_name, dst_last_name);
    }
}
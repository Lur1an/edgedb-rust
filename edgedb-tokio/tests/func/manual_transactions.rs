use edgedb_errors::NoDataError;
use edgedb_tokio::Client;

use crate::server::SERVER;

#[tokio::test]
async fn queries() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    let mut tx = client.start_transaction().await?;
    let value = tx.query::<i64, _>("SELECT 7*93", &()).await?;
    assert_eq!(value, vec![651]);

    let value = tx.query_single::<i64, _>("SELECT 5*11", &()).await?;
    assert_eq!(value, Some(55));

    let value = tx.query_single::<i64, _>("SELECT <int64>{}", &()).await?;
    assert_eq!(value, None);

    let value = tx
        .query_required_single::<i64, _>("SELECT 5*11", &())
        .await?;
    assert_eq!(value, 55);

    let err = tx
        .query_required_single::<i64, _>("SELECT <int64>{}", &())
        .await
        .unwrap_err();
    assert!(err.is::<NoDataError>());

    let value = tx.query_json("SELECT 'x' ++ 'y'", &()).await?;
    assert_eq!(value.as_ref(), r#"["xy"]"#);

    let value = tx.query_single_json("SELECT 'x' ++ 'y'", &()).await?;
    assert_eq!(value.as_deref(), Some(r#""xy""#));

    let value = tx.query_single_json("SELECT <str>{}", &()).await?;
    assert_eq!(value.as_deref(), None);

    let err = tx
        .query_required_single_json("SELECT <int64>{}", &())
        .await
        .unwrap_err();
    assert!(err.is::<NoDataError>());

    tx.execute("SELECT 1+1", &()).await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn rollback() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    let retrieve_query = "
        with
            counter := (select test::Counter filter .name = 'deez')
        select((counter.name, counter.value))
    ";
    {
        let mut tx = client.start_transaction().await?;
        let query = "
            with
                counter := (insert test::Counter { name := 'deez', value := 420 })
            select((counter.name, counter.value))
        ";
        tx.execute(query, &()).await?;
        let (name, value) = tx
            .query_required_single::<(String, i32), _>(retrieve_query, &())
            .await?;
        assert_eq!(name, "deez");
        assert_eq!(value, 420);
    }
    let should_not_exist = client
        .query_single::<(String, i32), _>(retrieve_query, &())
        .await?;
    assert!(should_not_exist.is_none());
    Ok(())
}

#[tokio::test]
async fn commit() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    {
        let mut tx = client.start_transaction().await?;
        let query = "
            with
                counter := (insert test::Counter { name := 'peanuts', value := 420 })
            select((counter.name, counter.value))
        ";
        tx.execute(query, &()).await?;
        tx.commit().await?;
    }
    let query = "
        with
            counter := (select test::Counter filter .name = 'peanuts')
        select((counter.name, counter.value))
    ";
    let (name, value) = client
        .query_required_single::<(String, i32), _>(query, &())
        .await?;
    assert_eq!(name, "peanuts");
    assert_eq!(value, 420);
    Ok(())
}

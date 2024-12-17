use crate::Nattes;
use futures::StreamExt;

#[tokio::test]
async fn simple() -> anyhow::Result<()> {
    let nattes = Nattes::new();
    let mut j = nattes.subscribe("zebi".parse()?).await?;
    nattes.publish("zebi".parse()?, "lol").await?;

    let result = j.next().await;

    assert!(result.is_some(), "Expected message to be sent");
    assert_eq!(result.unwrap(), "lol".as_bytes());

    nattes.publish("zeb".parse()?, "lol2").await?;
    nattes.publish("zebi".parse()?, "lol3").await?;
    let result = j.next().await;
    assert!(result.is_some(), "Expected message to be sent");
    assert_eq!(
        result.unwrap(),
        "lol3".as_bytes(),
        "Expected message to be sent"
    );
    Ok(())
}

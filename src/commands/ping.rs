use bytes::Bytes;

use crate::{Handler, Resp};

use super::IterResp;

#[derive(Debug)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    pub(crate) const fn new(msg: Option<Bytes>) -> Self {
        Self { msg }
    }

    pub(super) fn parse(mut i: IterResp) -> Self {
        let msg = i.next().and_then(Resp::as_bulk).cloned();
        Self { msg }
    }

    pub async fn apply_and_respond(self, handler: &mut Handler) -> anyhow::Result<()> {
        let msg = self.msg.map_or_else(|| Resp::simple("PONG"), Resp::Bulk);
        handler.write(&msg).await?;
        Ok(())
    }

    pub(crate) fn into_resp(self) -> Resp {
        let mut v = vec![Resp::bulk("PING")];
        if let Some(msg) = self.msg {
            v.push(Resp::Bulk(msg));
        }
        Resp::Array(v)
    }
}

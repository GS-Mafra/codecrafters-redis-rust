mod ping;
pub use ping::Ping;

mod echo;
pub use echo::Echo;

mod get;
pub use get::Get;

mod set;
use set::Set;

mod del;
pub use del::Del;

mod info;
pub use info::Info;

mod replconf;
pub use replconf::ReplConf;

mod wait;
pub use wait::Wait;

mod psync;
pub use psync::Psync;

mod config;
pub use config::Config;

use anyhow::bail;

use crate::Resp;

type IterResp<'a> = std::slice::Iter<'a, Resp>;

pub enum Command<'a> {
    Ping(Ping),
    Echo(Echo<'a>),
    Get(Get),
    Set(Set),
    Del(Del),
    Info(Info),
    ReplConf(ReplConf),
    Wait(Wait),
    Psync(Psync),
    Config(Config<'a>),
}

impl<'a> Command<'a> {
    pub fn parse(resp: &'a Resp) -> anyhow::Result<(Self, &'a [Resp])> {
        let Some(raw_cmd) = resp.as_array() else {
            bail!("Unsupported RESP for command");
        };

        let mut values = raw_cmd.iter();
        let Some(command) = values.next().and_then(Resp::as_bulk) else {
            bail!("Expected bulk string");
        };

        let parsed_cmd = match command.to_ascii_lowercase().as_slice() {
            b"ping" => Self::Ping(Ping::parse(values)),
            b"echo" => Self::Echo(Echo::parse(values)?),
            b"get" => Self::Get(Get::parse(values)?),
            b"set" => Self::Set(Set::parse(values)?),
            b"del" => Self::Del(Del::parse(values)),
            b"info" => Self::Info(Info::parse(values)),
            b"replconf" => Self::ReplConf(ReplConf::parse(values)?),
            b"wait" => Self::Wait(Wait::parse(values)?),
            b"psync" => Self::Psync(Psync::parse(values)?),
            b"config" => Self::Config(Config::parse(values)?),
            _ => unimplemented!("{command:?}"),
        };
        Ok((parsed_cmd, raw_cmd))
    }
}

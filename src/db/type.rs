use bytes::Bytes;

use super::Stream;

#[derive(Debug)]
#[repr(u8)]
pub enum Type {
    String(Bytes) = 0,
    // List,
    // Set,
    // SortedSet,
    // Hash,
    // Zipmap,
    // Ziplist,
    // Intset Encoding,
    // Sorted Set in Ziplist Encoding,
    // Hashmap in Ziplist Encoding,
    // List in Quicklist Encoding,
    Stream(Stream) = 21,
}

impl Type {
    #[inline]
    pub(crate) const fn as_string(&self) -> Option<&Bytes> {
        #[allow(clippy::match_wildcard_for_single_variants)]
        match self {
            Self::String(string) => Some(string),
            _ => None,
        }
    }
}

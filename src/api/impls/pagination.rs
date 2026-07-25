use std::cmp::Ordering;
use std::fmt::Display;
use std::time::SystemTime;

use base64::{engine::general_purpose::URL_SAFE, Engine};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginationCursor<T> {
    time: SystemTime,
    value: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paginated<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PaginationError {
    #[error("error decoding cursor: {0}")]
    DecodeCursor(String),
    #[error("invalid cursor format")]
    InvalidCursorFormat,
    #[error("invalid cursor format (not utf-8): {0}")]
    InvalidCursorUtf8(String),
    #[error("invalid timestamp format in cursor: {0}")]
    InvalidCursorTimestamp(String),
    #[error("invalid cursor value: {0}")]
    InvalidCursorValue(String),
}

impl<T> PaginationCursor<T> {
    pub fn new(time: SystemTime, value: T) -> Self {
        Self { time, value }
    }

    pub fn time(&self) -> SystemTime {
        self.time
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn parse(token: &str) -> Result<Self, PaginationError>
    where
        T: for<'a> TryFrom<&'a str>,
        for<'a> <T as TryFrom<&'a str>>::Error: ToString,
    {
        let decoded = URL_SAFE
            .decode(token)
            .map_err(|err| PaginationError::DecodeCursor(err.to_string()))?;
        let decoded = String::from_utf8(decoded)
            .map_err(|err| PaginationError::InvalidCursorUtf8(err.to_string()))?;
        let (timestamp, value) = decoded
            .split_once("__")
            .ok_or(PaginationError::InvalidCursorFormat)?;
        let cursor_time = chrono::DateTime::parse_from_rfc3339(timestamp)
            .map_err(|err| PaginationError::InvalidCursorTimestamp(err.to_string()))?
            .with_timezone(&chrono::Utc);
        let value = T::try_from(value)
            .map_err(|err| PaginationError::InvalidCursorValue(err.to_string()))?;

        Ok(Self::new(SystemTime::from(cursor_time), value))
    }

    pub fn paginate<Item>(
        &self,
        mut items: Vec<Item>,
        limit: Option<u32>,
        mut sort: impl FnMut(&Item, &Item) -> Ordering,
        compare_cursor: impl FnMut(&Item, &Self) -> Ordering,
        to_cursor: impl FnMut(&Item) -> Self,
    ) -> Paginated<Item>
    where
        T: Display,
    {
        if matches!(limit, Some(0)) {
            return Paginated {
                items: Vec::new(),
                next_token: None,
            };
        }

        items.sort_by(|a, b| sort(a, b));
        self.paginate_sorted(items, limit, compare_cursor, to_cursor)
    }

    pub fn paginate_sorted<Item>(
        &self,
        mut items: Vec<Item>,
        limit: Option<u32>,
        mut compare_cursor: impl FnMut(&Item, &Self) -> Ordering,
        mut to_cursor: impl FnMut(&Item) -> Self,
    ) -> Paginated<Item>
    where
        T: Display,
    {
        if matches!(limit, Some(0)) {
            return Paginated {
                items: Vec::new(),
                next_token: None,
            };
        }

        items.retain(|item| compare_cursor(item, self) == Ordering::Greater);

        let next_token = match limit.and_then(|l| usize::try_from(l).ok()) {
            Some(limit) if items.len() > limit => {
                let token = items.get(limit - 1).map(|item| to_cursor(item).encode());
                items.truncate(limit);
                token
            }
            _ => None,
        };

        Paginated { items, next_token }
    }
}

impl<T> PaginationCursor<T>
where
    T: Display,
{
    pub fn encode(&self) -> String {
        URL_SAFE.encode(format!(
            "{}__{}",
            chrono::DateTime::<chrono::Utc>::from(self.time)
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            self.value,
        ))
    }
}

impl<T> PaginationCursor<T>
where
    T: Ord,
{
    pub fn compare_desc(
        a_time: SystemTime,
        a_value: &T,
        b_time: SystemTime,
        b_value: &T,
    ) -> Ordering {
        b_time.cmp(&a_time).then_with(|| a_value.cmp(b_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestId(u32);

    impl std::fmt::Display for TestId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    impl TryFrom<&str> for TestId {
        type Error = std::num::ParseIntError;

        fn try_from(value: &str) -> Result<Self, Self::Error> {
            value.parse().map(Self)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestItem {
        created_at: SystemTime,
        id: TestId,
    }

    fn item(id: u32, created_at: SystemTime) -> TestItem {
        TestItem {
            created_at,
            id: TestId(id),
        }
    }

    fn sort_test_items(a: &TestItem, b: &TestItem) -> Ordering {
        PaginationCursor::compare_desc(a.created_at, &a.id, b.created_at, &b.id)
    }

    fn compare_test_item_cursor(item: &TestItem, cursor: &PaginationCursor<TestId>) -> Ordering {
        PaginationCursor::compare_desc(item.created_at, &item.id, cursor.time(), cursor.value())
    }

    fn cursor_for_test_item(item: &TestItem) -> PaginationCursor<TestId> {
        PaginationCursor::new(item.created_at, item.id.clone())
    }

    fn encode_raw_cursor(timestamp: &str, value: &str) -> String {
        URL_SAFE.encode(format!("{timestamp}__{value}"))
    }

    #[test]
    fn parse_next_token_valid() {
        let ts = UNIX_EPOCH + Duration::from_secs(1_767_225_600);
        let token = encode_raw_cursor("2026-01-01T00:00:00Z", "2");

        let cursor = PaginationCursor::<TestId>::parse(&token).unwrap();

        assert_eq!(cursor.time(), ts);
        assert_eq!(cursor.value().to_string(), "2");
    }

    #[test]
    fn parse_next_token_invalid_returns_error() {
        let err = PaginationCursor::<TestId>::parse("not-base64").unwrap_err();
        assert!(matches!(err, PaginationError::DecodeCursor(_)));
    }

    #[test]
    fn parse_next_token_empty_returns_error() {
        let err = PaginationCursor::<TestId>::parse("").unwrap_err();
        assert_eq!(err, PaginationError::InvalidCursorFormat);
    }

    #[test]
    fn parse_next_token_invalid_format_returns_error() {
        let token = URL_SAFE.encode("2026-01-01T00:00:00Z-only");
        let err = PaginationCursor::<TestId>::parse(&token).unwrap_err();
        assert_eq!(err, PaginationError::InvalidCursorFormat);
    }

    #[test]
    fn parse_next_token_invalid_utf8_returns_error() {
        let token = URL_SAFE.encode([0xff, 0xfe, 0xfd]);
        let err = PaginationCursor::<TestId>::parse(&token).unwrap_err();
        assert!(matches!(err, PaginationError::InvalidCursorUtf8(_)));
    }

    #[test]
    fn parse_next_token_invalid_timestamp_returns_error() {
        let token = URL_SAFE.encode("bad-timestamp__1");
        let err = PaginationCursor::<TestId>::parse(&token).unwrap_err();
        assert!(matches!(err, PaginationError::InvalidCursorTimestamp(_)));
    }

    #[test]
    fn parse_next_token_invalid_value_returns_error() {
        let token = encode_raw_cursor("2026-01-01T00:00:00Z", "not-a-number");
        let err = PaginationCursor::<TestId>::parse(&token).unwrap_err();
        assert!(matches!(err, PaginationError::InvalidCursorValue(_)));
    }

    #[test]
    fn paginate_sorts_and_filters_by_cursor() {
        let t1 = UNIX_EPOCH + Duration::from_secs(200);
        let t2 = UNIX_EPOCH + Duration::from_secs(100);

        let items = vec![item(3, t1), item(1, t1), item(2, t2), item(4, t2)];

        let cursor = PaginationCursor::new(t1, TestId(1));
        let out = cursor.paginate(
            items,
            Some(2),
            sort_test_items,
            compare_test_item_cursor,
            cursor_for_test_item,
        );

        let ids: Vec<_> = out.items.into_iter().map(|m| m.id.0).collect();
        assert_eq!(ids, vec![3, 2]);
    }

    #[test]
    fn paginate_returns_none_next_token_when_page_not_full_or_limit_missing_or_invalid() {
        let t = UNIX_EPOCH + Duration::from_secs(100);
        let items = vec![item(1, t)];
        let cursor = PaginationCursor::new(SystemTime::now(), TestId(u32::MAX));

        let paginate = |limit| {
            cursor.paginate(
                items.clone(),
                limit,
                sort_test_items,
                compare_test_item_cursor,
                cursor_for_test_item,
            )
        };

        assert_eq!(paginate(Some(2)).next_token, None);
        assert_eq!(paginate(Some(3)).next_token, None);
        assert_eq!(paginate(Some(0)).next_token, None);
        assert_eq!(paginate(None).next_token, None);
    }

    #[test]
    fn paginate_returns_next_token_when_more_items_than_limit() {
        let t1 = UNIX_EPOCH + Duration::from_secs(200);
        let t2 = UNIX_EPOCH + Duration::from_secs(100);
        let t3 = UNIX_EPOCH + Duration::from_secs(50);
        let out = PaginationCursor::new(SystemTime::now(), TestId(u32::MAX)).paginate(
            vec![item(3, t1), item(2, t2), item(1, t3)],
            Some(2),
            sort_test_items,
            compare_test_item_cursor,
            cursor_for_test_item,
        );

        assert_eq!(out.items.len(), 2);
        let decoded =
            String::from_utf8(URL_SAFE.decode(out.next_token.expect("token")).unwrap()).unwrap();
        assert_eq!(decoded, "1970-01-01T00:01:40.000000000Z__2");
    }

    #[test]
    fn paginate_returns_no_token_when_items_exactly_fill_page() {
        let t1 = UNIX_EPOCH + Duration::from_secs(200);
        let t2 = UNIX_EPOCH + Duration::from_secs(100);
        let out = PaginationCursor::new(SystemTime::now(), TestId(u32::MAX)).paginate(
            vec![item(3, t1), item(2, t2)],
            Some(2),
            sort_test_items,
            compare_test_item_cursor,
            cursor_for_test_item,
        );

        assert_eq!(out.items.len(), 2);
        assert_eq!(out.next_token, None);
    }

    #[test]
    fn compare_desc_orders_by_time_desc_then_value_asc() {
        let t1 = UNIX_EPOCH + Duration::from_secs(100);
        let t2 = UNIX_EPOCH + Duration::from_secs(200);
        let id1 = TestId(1);
        let id2 = TestId(2);

        assert_eq!(
            PaginationCursor::compare_desc(t2, &id1, t1, &id2),
            Ordering::Less
        );
        assert_eq!(
            PaginationCursor::compare_desc(t1, &id1, t1, &id2),
            Ordering::Less
        );
    }
}

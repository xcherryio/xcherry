// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! S3 XML documents: a small escaping writer, request-body parsing, and S3 timestamps.

use quick_xml::events::Event;

use crate::s3_error::S3Error;

pub(crate) const S3_NAMESPACE: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Builds an XML document; every text value is escaped.
pub(crate) struct XmlWriter {
    document: String,
}

impl XmlWriter {
    pub(crate) fn new(root: &str) -> XmlWriter {
        XmlWriter {
            document: format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root} xmlns=\"{S3_NAMESPACE}\">"
            ),
        }
    }

    /// Starts a document whose root has no namespace, as S3 error documents do.
    pub(crate) fn without_namespace(root: &str) -> XmlWriter {
        XmlWriter {
            document: format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root}>"),
        }
    }

    pub(crate) fn open(&mut self, element: &str) -> &mut XmlWriter {
        self.document.push('<');
        self.document.push_str(element);
        self.document.push('>');
        self
    }

    pub(crate) fn close(&mut self, element: &str) -> &mut XmlWriter {
        self.document.push_str("</");
        self.document.push_str(element);
        self.document.push('>');
        self
    }

    pub(crate) fn text_element(&mut self, element: &str, text: &str) -> &mut XmlWriter {
        self.open(element);
        self.document.push_str(&quick_xml::escape::escape(text));
        self.close(element)
    }

    pub(crate) fn optional_text_element(
        &mut self,
        element: &str,
        text: Option<&str>,
    ) -> &mut XmlWriter {
        if let Some(text) = text {
            self.text_element(element, text);
        }
        self
    }

    pub(crate) fn finish(mut self, root: &str) -> String {
        self.close(root);
        self.document
    }
}

/// Returns the text of the first `element` in an XML body, or `None` when it is absent.
pub(crate) fn first_element_text(body: &[u8], element: &str) -> Result<Option<String>, S3Error> {
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut buffer = Vec::new();
    let mut is_inside_element = false;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(start)) => {
                is_inside_element = start.local_name().as_ref() == element.as_bytes()
            }
            Ok(Event::Text(text)) if is_inside_element => {
                let unescaped = text.unescape().map_err(|error| {
                    S3Error::malformed_xml(format!("invalid XML text: {error}"))
                })?;
                return Ok(Some(unescaped.trim().to_owned()));
            }
            Ok(Event::End(_)) => {
                if is_inside_element {
                    return Ok(Some(String::new()));
                }
            }
            Ok(Event::Eof) => return Ok(None),
            Ok(_) => {}
            Err(error) => {
                return Err(S3Error::malformed_xml(format!(
                    "the XML is not well formed: {error}"
                )));
            }
        }
        buffer.clear();
    }
}

/// Formats Unix milliseconds as `2006-02-03T16:45:09.000Z`, the timestamp format of S3 XML.
pub(crate) fn format_iso8601(unix_ms: i64) -> String {
    let seconds = unix_ms.div_euclid(1000);
    let milliseconds = unix_ms.rem_euclid(1000);
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{milliseconds:03}Z",
        second_of_day / 3600,
        second_of_day % 3600 / 60,
        second_of_day % 60
    )
}

/// Converts days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_s3_timestamps() {
        assert_eq!(format_iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_iso8601(951_782_400_123), "2000-02-29T00:00:00.123Z");
        assert_eq!(
            format_iso8601(1_792_627_200_000),
            "2026-10-22T00:00:00.000Z"
        );
    }
}

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

/// One element of a parsed request document.
#[derive(Debug, Default)]
pub(crate) struct XmlElement {
    pub(crate) name: String,
    /// Character data directly inside this element, concatenated across text and CDATA sections.
    pub(crate) text: String,
    pub(crate) children: Vec<XmlElement>,
}

impl XmlElement {
    pub(crate) fn child(&self, name: &str) -> Option<&XmlElement> {
        self.children.iter().find(|child| child.name == name)
    }

    pub(crate) fn children_named<'element>(
        &'element self,
        name: &'element str,
    ) -> impl Iterator<Item = &'element XmlElement> {
        self.children.iter().filter(move |child| child.name == name)
    }

    /// Returns the untrimmed text of the first direct child named `name`, which must be a leaf.
    pub(crate) fn child_text(&self, name: &str) -> Result<Option<&str>, S3Error> {
        match self.child(name) {
            None => Ok(None),
            Some(child) if child.children.is_empty() => Ok(Some(child.text.as_str())),
            Some(child) => Err(S3Error::malformed_xml(format!(
                "{} must contain text, not elements",
                child.name
            ))),
        }
    }
}

/// Parses one complete document whose root element is `root`; anything else is MalformedXML.
pub(crate) fn parse_document(body: &[u8], root: &str) -> Result<XmlElement, S3Error> {
    let malformed = |detail: String| {
        S3Error::malformed_xml(format!(
            "The XML you provided was not well-formed or did not validate against our published schema: {detail}"
        ))
    };
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut buffer = Vec::new();
    let mut open_elements: Vec<XmlElement> = Vec::new();
    let mut document: Option<XmlElement> = None;
    loop {
        let event = reader
            .read_event_into(&mut buffer)
            .map_err(|error| malformed(error.to_string()))?;
        match event {
            Event::Start(start) => {
                if document.is_some() {
                    return Err(malformed("content after the root element".to_owned()));
                }
                let name = String::from_utf8_lossy(start.local_name().as_ref()).into_owned();
                if open_elements.is_empty() && name != root {
                    return Err(malformed(format!("the root element must be {root}")));
                }
                open_elements.push(XmlElement {
                    name,
                    ..XmlElement::default()
                });
            }
            Event::Empty(empty) => {
                let element = XmlElement {
                    name: String::from_utf8_lossy(empty.local_name().as_ref()).into_owned(),
                    ..XmlElement::default()
                };
                match open_elements.last_mut() {
                    Some(parent) => parent.children.push(element),
                    None if document.is_none() && element.name == root => document = Some(element),
                    None => return Err(malformed(format!("the root element must be {root}"))),
                }
            }
            Event::End(end) => {
                let element = open_elements
                    .pop()
                    .ok_or_else(|| malformed("an unmatched end tag".to_owned()))?;
                if end.local_name().as_ref() != element.name.as_bytes() {
                    return Err(malformed(format!(
                        "mismatched end tag for {}",
                        element.name
                    )));
                }
                // S3 documents never mix character data with child elements.
                if !element.children.is_empty() && !element.text.trim().is_empty() {
                    return Err(malformed(format!(
                        "{} mixes text and elements",
                        element.name
                    )));
                }
                match open_elements.last_mut() {
                    Some(parent) => parent.children.push(element),
                    None => document = Some(element),
                }
            }
            Event::Text(text) => {
                let unescaped = text
                    .unescape()
                    .map_err(|error| malformed(format!("invalid character data: {error}")))?;
                match open_elements.last_mut() {
                    Some(element) => element.text.push_str(&unescaped),
                    None if unescaped.trim().is_empty() => {}
                    None => return Err(malformed("text outside the root element".to_owned())),
                }
            }
            Event::CData(cdata) => {
                let text = std::str::from_utf8(&cdata)
                    .map_err(|_| malformed("CDATA is not valid UTF-8".to_owned()))?;
                match open_elements.last_mut() {
                    Some(element) => element.text.push_str(text),
                    None => return Err(malformed("CDATA outside the root element".to_owned())),
                }
            }
            Event::Eof => {
                if !open_elements.is_empty() {
                    return Err(malformed(
                        "the document ended before its root element closed".to_owned(),
                    ));
                }
                return document
                    .ok_or_else(|| malformed(format!("the document has no {root} element")));
            }
            Event::Decl(_) | Event::Comment(_) | Event::PI(_) | Event::DocType(_) => {}
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

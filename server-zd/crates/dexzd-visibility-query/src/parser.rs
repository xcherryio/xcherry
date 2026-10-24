// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::num::{IntErrorKind, ParseIntError};

use dexzd_protocol::dex::IndexType;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::lexer::{Token, TokenKind, lex};
use crate::{
    Expression, FieldRegistry, Literal, Operator, OrderBy, Predicate, Query, QueryError,
    SortDirection,
};

const MAX_QUERY_BYTES: usize = 256 * 1024;
const MAX_EXPRESSIONS: usize = 10_000;
const MAX_NESTING: usize = 64;

pub(crate) fn parse(input: &str, fields: &FieldRegistry) -> Result<Query, QueryError> {
    if input.len() > MAX_QUERY_BYTES {
        return Err(QueryError::new(MAX_QUERY_BYTES, "query exceeds 256 KiB"));
    }
    let tokens = lex(input)?;
    let mut parser = Parser {
        tokens,
        position: 0,
        fields,
        expression_count: 0,
        nesting_depth: 0,
    };
    let expression = if parser.at_end() || parser.at_keyword("ORDER") {
        Expression::True
    } else {
        parser.parse_or()?
    };
    let order_by = if parser.take_keyword("ORDER") {
        parser.expect_keyword("BY")?;
        let field_token = parser.advance().clone();
        let TokenKind::Identifier(field) = field_token.kind else {
            return Err(QueryError::new(
                field_token.offset,
                "ORDER BY expects StartTime or CloseTime",
            ));
        };
        if field != "StartTime" && field != "CloseTime" {
            return Err(QueryError::new(
                field_token.offset,
                format!("unsupported ORDER BY field {field:?}; expected StartTime or CloseTime"),
            ));
        }
        let direction = if parser.take_keyword("DESC") {
            SortDirection::Descending
        } else {
            parser.take_keyword("ASC");
            SortDirection::Ascending
        };
        Some(OrderBy { field, direction })
    } else {
        None
    };
    if !parser.at_end() {
        return Err(QueryError::new(
            parser.current().offset,
            format!(
                "unsupported trailing query part {}",
                parser.describe_current()
            ),
        ));
    }
    Ok(Query {
        expression,
        order_by,
    })
}

struct Parser<'a> {
    tokens: Vec<Token>,
    position: usize,
    fields: &'a FieldRegistry,
    expression_count: usize,
    nesting_depth: usize,
}

impl Parser<'_> {
    fn parse_or(&mut self) -> Result<Expression, QueryError> {
        let mut expressions = vec![self.parse_and()?];
        while self.take_keyword("OR") {
            expressions.push(self.parse_and()?);
        }
        if expressions.len() == 1 {
            Ok(expressions.remove(0))
        } else {
            Ok(Expression::Or(expressions))
        }
    }

    fn parse_and(&mut self) -> Result<Expression, QueryError> {
        let mut expressions = vec![self.parse_not()?];
        while self.take_keyword("AND") {
            expressions.push(self.parse_not()?);
        }
        if expressions.len() == 1 {
            Ok(expressions.remove(0))
        } else {
            Ok(Expression::And(expressions))
        }
    }

    fn parse_not(&mut self) -> Result<Expression, QueryError> {
        if self.take_keyword("NOT") {
            self.enter_nesting(self.tokens[self.position - 1].offset)?;
            let inner = self.parse_not()?;
            self.nesting_depth -= 1;
            return Ok(Expression::Not(Box::new(inner)));
        }
        if self.take_symbol("(") {
            self.enter_nesting(self.tokens[self.position - 1].offset)?;
            let expression = self.parse_or()?;
            self.expect_symbol(")")?;
            self.nesting_depth -= 1;
            return Ok(expression);
        }
        self.parse_predicate()
    }

    fn enter_nesting(&mut self, offset: usize) -> Result<(), QueryError> {
        if self.nesting_depth >= MAX_NESTING {
            return Err(QueryError::new(offset, "query nesting exceeds 64 levels"));
        }
        self.nesting_depth += 1;
        Ok(())
    }

    fn parse_predicate(&mut self) -> Result<Expression, QueryError> {
        let field_token = self.advance().clone();
        if self.expression_count >= MAX_EXPRESSIONS {
            return Err(QueryError::new(
                field_token.offset,
                "query exceeds 10000 expressions",
            ));
        }
        self.expression_count += 1;
        let (TokenKind::Identifier(field) | TokenKind::QuotedIdentifier(field)) = field_token.kind
        else {
            return Err(QueryError::new(
                field_token.offset,
                format!("expected field name, found {}", describe(&field_token.kind)),
            ));
        };
        let Some(index_type) = self.fields.index_type(&field) else {
            return Err(QueryError::new(
                field_token.offset,
                format!("unknown visibility field {field:?}"),
            ));
        };
        if index_type == IndexType::Unspecified {
            return Err(QueryError::new(
                field_token.offset,
                format!("unsupported index type for {field:?}"),
            ));
        }
        let operator_offset = self.current().offset;
        let operator = if self.take_keyword("IS") {
            if self.take_keyword("NOT") {
                self.expect_keyword("NULL")?;
                Operator::IsNotNull
            } else {
                self.expect_keyword("NULL")?;
                Operator::IsNull
            }
        } else if self.take_keyword("NOT") {
            self.expect_keyword("IN")?;
            Operator::NotIn
        } else if self.take_keyword("IN") {
            Operator::In
        } else if self.take_keyword("BETWEEN") {
            Operator::Between
        } else if self.take_keyword("STARTS_WITH") {
            Operator::StartsWith
        } else {
            let token = self.advance().clone();
            match token.kind {
                TokenKind::Symbol("=") => Operator::Equal,
                TokenKind::Symbol("!=") | TokenKind::Symbol("<>") => Operator::NotEqual,
                TokenKind::Symbol(">") => Operator::Greater,
                TokenKind::Symbol(">=") => Operator::GreaterEqual,
                TokenKind::Symbol("<") => Operator::Less,
                TokenKind::Symbol("<=") => Operator::LessEqual,
                _ => {
                    return Err(QueryError::new(
                        token.offset,
                        format!("unsupported operator {} for {field}", describe(&token.kind)),
                    ));
                }
            }
        };
        self.check_operator(&field, index_type, operator, operator_offset)?;
        let literals = match operator {
            Operator::IsNull | Operator::IsNotNull => Vec::new(),
            Operator::In | Operator::NotIn => {
                self.expect_symbol("(")?;
                let mut values = Vec::new();
                loop {
                    values.push(self.parse_literal(&field, index_type, operator)?);
                    if !self.take_symbol(",") {
                        break;
                    }
                }
                self.expect_symbol(")")?;
                values
            }
            Operator::Between => {
                let lower = self.parse_literal(&field, index_type, operator)?;
                self.expect_keyword("AND")?;
                let upper = self.parse_literal(&field, index_type, operator)?;
                vec![lower, upper]
            }
            _ => vec![self.parse_literal(&field, index_type, operator)?],
        };
        let predicate = Predicate {
            field,
            index_type,
            operator,
            literals: Vec::new(),
        };
        if index_type == IndexType::Int {
            rewrite_int_predicate(predicate, literals)
        } else {
            Ok(Expression::Predicate(Predicate {
                literals: literals
                    .into_iter()
                    .map(|literal| match literal {
                        ParsedLiteral::Literal(value) => value,
                        ParsedLiteral::FractionalInt { .. } => {
                            unreachable!("fractional INT literal in another index type")
                        }
                    })
                    .collect(),
                ..predicate
            }))
        }
    }

    fn check_operator(
        &self,
        field: &str,
        index_type: IndexType,
        operator: Operator,
        offset: usize,
    ) -> Result<(), QueryError> {
        let allowed = match operator {
            Operator::IsNull | Operator::IsNotNull => true,
            Operator::Equal | Operator::NotEqual | Operator::In | Operator::NotIn => true,
            Operator::Greater
            | Operator::GreaterEqual
            | Operator::Less
            | Operator::LessEqual
            | Operator::Between => matches!(
                index_type,
                IndexType::Int | IndexType::Double | IndexType::Datetime | IndexType::Keyword
            ),
            Operator::StartsWith => index_type == IndexType::Keyword,
        };
        if allowed {
            Ok(())
        } else {
            Err(QueryError::new(
                offset,
                format!("operator {operator:?} is unsupported for {field} ({index_type:?})"),
            ))
        }
    }

    fn parse_literal(
        &mut self,
        field: &str,
        index_type: IndexType,
        operator: Operator,
    ) -> Result<ParsedLiteral, QueryError> {
        let token = self.advance().clone();
        let literal = match (&token.kind, index_type) {
            (
                TokenKind::String(value) | TokenKind::Number(value),
                IndexType::Keyword | IndexType::Text | IndexType::KeywordArray,
            ) => Literal::String(value.clone()),
            (TokenKind::String(value), IndexType::Datetime) => {
                let timestamp = OffsetDateTime::parse(value, &Rfc3339)
                    .map_err(|_| {
                        QueryError::new(
                            token.offset,
                            format!("invalid RFC3339 DATETIME literal for {field}"),
                        )
                    })?
                    .unix_timestamp_nanos();
                Literal::Datetime(i64::try_from(timestamp).map_err(|_| {
                    QueryError::new(
                        token.offset,
                        format!("DATETIME literal out of range for {field}"),
                    )
                })?)
            }
            (TokenKind::Number(value), IndexType::Int)
                if !value.contains('.') && !value.contains('e') && !value.contains('E') =>
            {
                Literal::Int(parse_int_literal(value, field, token.offset)?)
            }
            // Temporal visibility accepts quoted numeric literals for INT and DOUBLE fields.
            (TokenKind::String(value), IndexType::Int) => {
                return parse_quoted_int_literal(value, field, token.offset);
            }
            (TokenKind::Number(value) | TokenKind::String(value), IndexType::Double) => {
                Literal::Double(parse_double_literal(value, field, token.offset)?)
            }
            (TokenKind::Identifier(value) | TokenKind::String(value), IndexType::Bool)
                if value.eq_ignore_ascii_case("true") =>
            {
                Literal::Bool(true)
            }
            (TokenKind::Identifier(value) | TokenKind::String(value), IndexType::Bool)
                if value.eq_ignore_ascii_case("false") =>
            {
                Literal::Bool(false)
            }
            _ => {
                return Err(QueryError::new(
                    token.offset,
                    format!(
                        "expected {index_type:?} literal for {field}, found {}",
                        describe(&token.kind)
                    ),
                ));
            }
        };
        if field == "ExecutionStatus"
            && matches!(
                operator,
                Operator::Equal | Operator::NotEqual | Operator::In | Operator::NotIn
            )
            && let Literal::String(value) = &literal
            && !matches!(
                value.as_str(),
                "Running"
                    | "Completed"
                    | "Failed"
                    | "Canceled"
                    | "Terminated"
                    | "TimedOut"
                    | "ContinuedAsNew"
            )
        {
            return Err(QueryError::new(
                token.offset,
                format!("unsupported ExecutionStatus value {value:?}"),
            ));
        }
        Ok(ParsedLiteral::Literal(literal))
    }

    fn current(&self) -> &Token {
        &self.tokens[self.position]
    }
    fn at_end(&self) -> bool {
        matches!(self.current().kind, TokenKind::End)
    }
    fn at_keyword(&self, keyword: &str) -> bool {
        matches!(&self.current().kind, TokenKind::Identifier(value) if value.eq_ignore_ascii_case(keyword))
    }
    fn take_keyword(&mut self, keyword: &str) -> bool {
        if self.at_keyword(keyword) {
            self.advance();
            true
        } else {
            false
        }
    }
    fn expect_keyword(&mut self, keyword: &str) -> Result<(), QueryError> {
        if self.take_keyword(keyword) {
            Ok(())
        } else {
            Err(QueryError::new(
                self.current().offset,
                format!("expected {keyword}, found {}", self.describe_current()),
            ))
        }
    }
    fn take_symbol(&mut self, symbol: &str) -> bool {
        if matches!(&self.current().kind, TokenKind::Symbol(value) if *value == symbol) {
            self.advance();
            true
        } else {
            false
        }
    }
    fn expect_symbol(&mut self, symbol: &str) -> Result<(), QueryError> {
        if self.take_symbol(symbol) {
            Ok(())
        } else {
            Err(QueryError::new(
                self.current().offset,
                format!("expected {symbol}, found {}", self.describe_current()),
            ))
        }
    }
    fn advance(&mut self) -> &Token {
        let position = self.position;
        if !self.at_end() {
            self.position += 1;
        }
        &self.tokens[position]
    }
    fn describe_current(&self) -> String {
        describe(&self.current().kind)
    }
}

enum ParsedLiteral {
    Literal(Literal),
    FractionalInt { floor: i128 },
}

fn rewrite_int_predicate(
    mut predicate: Predicate,
    literals: Vec<ParsedLiteral>,
) -> Result<Expression, QueryError> {
    match predicate.operator {
        Operator::Equal | Operator::NotEqual => {
            if matches!(literals.first(), Some(ParsedLiteral::FractionalInt { .. })) {
                return Ok(if predicate.operator == Operator::Equal {
                    no_match_predicate(&predicate)
                } else {
                    present_predicate(&predicate)
                });
            }
        }
        Operator::In | Operator::NotIn => {
            predicate.literals = literals
                .into_iter()
                .filter_map(|literal| match literal {
                    ParsedLiteral::Literal(value) => Some(value),
                    ParsedLiteral::FractionalInt { .. } => None,
                })
                .collect();
            return if predicate.literals.is_empty() {
                Ok(if predicate.operator == Operator::In {
                    no_match_predicate(&predicate)
                } else {
                    present_predicate(&predicate)
                })
            } else {
                Ok(Expression::Predicate(predicate))
            };
        }
        Operator::Greater | Operator::GreaterEqual | Operator::Less | Operator::LessEqual => {
            if let Some(ParsedLiteral::FractionalInt { floor }) = literals.first() {
                let is_lower = matches!(
                    predicate.operator,
                    Operator::Greater | Operator::GreaterEqual
                );
                let bound = if is_lower { floor + 1 } else { *floor };
                if (is_lower && bound > i128::from(i64::MAX))
                    || (!is_lower && bound < i128::from(i64::MIN))
                {
                    return Ok(no_match_predicate(&predicate));
                }
                if (is_lower && bound <= i128::from(i64::MIN))
                    || (!is_lower && bound >= i128::from(i64::MAX))
                {
                    return Ok(present_predicate(&predicate));
                }
                predicate.operator = if is_lower {
                    Operator::GreaterEqual
                } else {
                    Operator::LessEqual
                };
                predicate.literals = vec![Literal::Int(
                    i64::try_from(bound).expect("fractional INT bound checked against i64"),
                )];
                return Ok(Expression::Predicate(predicate));
            }
        }
        Operator::Between => {
            let lower = match &literals[0] {
                ParsedLiteral::Literal(Literal::Int(value)) => i128::from(*value),
                ParsedLiteral::FractionalInt { floor } => floor + 1,
                ParsedLiteral::Literal(_) => unreachable!("BETWEEN INT literal must be numeric"),
            };
            let upper = match &literals[1] {
                ParsedLiteral::Literal(Literal::Int(value)) => i128::from(*value),
                ParsedLiteral::FractionalInt { floor } => *floor,
                ParsedLiteral::Literal(_) => unreachable!("BETWEEN INT literal must be numeric"),
            };
            if lower > upper || lower > i128::from(i64::MAX) || upper < i128::from(i64::MIN) {
                return Ok(no_match_predicate(&predicate));
            }
            predicate.literals = vec![
                Literal::Int(
                    i64::try_from(lower.max(i128::from(i64::MIN)))
                        .expect("lower INT bound clamped to i64"),
                ),
                Literal::Int(
                    i64::try_from(upper.min(i128::from(i64::MAX)))
                        .expect("upper INT bound clamped to i64"),
                ),
            ];
            return Ok(Expression::Predicate(predicate));
        }
        _ => {}
    }
    predicate.literals = literals
        .into_iter()
        .map(|literal| match literal {
            ParsedLiteral::Literal(value) => value,
            ParsedLiteral::FractionalInt { .. } => {
                unreachable!("fractional INT literal must be rewritten")
            }
        })
        .collect();
    Ok(Expression::Predicate(predicate))
}

fn no_match_predicate(predicate: &Predicate) -> Expression {
    Expression::NoMatch(Predicate {
        field: predicate.field.clone(),
        index_type: predicate.index_type,
        operator: Operator::IsNotNull,
        literals: Vec::new(),
    })
}

fn present_predicate(predicate: &Predicate) -> Expression {
    Expression::Predicate(Predicate {
        field: predicate.field.clone(),
        index_type: predicate.index_type,
        operator: Operator::IsNotNull,
        literals: Vec::new(),
    })
}

fn parse_quoted_int_literal(
    value: &str,
    field: &str,
    offset: usize,
) -> Result<ParsedLiteral, QueryError> {
    let bytes = value.as_bytes();
    let mut position = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let is_negative = bytes.first() == Some(&b'-');
    let mut digits = String::new();
    let mut fraction_digits = 0_i64;
    while bytes.get(position).is_some_and(u8::is_ascii_digit) {
        digits.push(char::from(bytes[position]));
        position += 1;
    }
    if bytes.get(position) == Some(&b'.') {
        position += 1;
        while bytes.get(position).is_some_and(u8::is_ascii_digit) {
            digits.push(char::from(bytes[position]));
            fraction_digits += 1;
            position += 1;
        }
    }
    if digits.is_empty() {
        return Err(invalid_numeric_literal(
            "INT",
            value,
            field,
            offset,
            "decimal number",
        ));
    }
    let exponent = if matches!(bytes.get(position), Some(b'e' | b'E')) {
        position += 1;
        match parse_signed_exponent(bytes, &mut position) {
            Some(exponent) => exponent,
            None => {
                return Err(invalid_numeric_literal(
                    "INT",
                    value,
                    field,
                    offset,
                    "decimal number",
                ));
            }
        }
    } else {
        0
    };
    if position != bytes.len() {
        return Err(invalid_numeric_literal(
            "INT",
            value,
            field,
            offset,
            "decimal number",
        ));
    }
    let significant = digits.trim_start_matches('0').trim_end_matches('0');
    if significant.is_empty() {
        return Ok(ParsedLiteral::Literal(Literal::Int(0)));
    }
    let trailing_zeros = digits.trim_start_matches('0').len() - significant.len();
    let scale = exponent
        .saturating_sub(fraction_digits)
        .saturating_add(i64::try_from(trailing_zeros).expect("query length fits i64"));
    if scale >= 0 {
        let integer_digits = i64::try_from(significant.len())
            .expect("query length fits i64")
            .saturating_add(scale);
        if integer_digits > 19 {
            return Err(QueryError::new(
                offset,
                format!("INT literal out of range for {field}"),
            ));
        }
        let mut integer = significant.to_owned();
        integer.extend(std::iter::repeat_n(
            '0',
            usize::try_from(scale).expect("bounded by i64 digit count"),
        ));
        if is_negative {
            integer.insert(0, '-');
        }
        let parsed = integer.parse::<i64>().map_err(|_| {
            QueryError::new(offset, format!("INT literal out of range for {field}"))
        })?;
        return Ok(ParsedLiteral::Literal(Literal::Int(parsed)));
    }
    let integer_digits = i64::try_from(significant.len())
        .expect("query length fits i64")
        .saturating_add(scale);
    let magnitude = if integer_digits <= 0 {
        0
    } else if integer_digits > 19 {
        i128::from(i64::MAX) + 1
    } else {
        significant[..usize::try_from(integer_digits).expect("positive digit count")]
            .parse::<i128>()
            .expect("at most 19 decimal digits")
            .min(i128::from(i64::MAX) + 1)
    };
    Ok(ParsedLiteral::FractionalInt {
        floor: if is_negative {
            -magnitude - 1
        } else {
            magnitude
        },
    })
}

fn parse_signed_exponent(bytes: &[u8], position: &mut usize) -> Option<i64> {
    let is_negative = match bytes.get(*position) {
        Some(b'-') => {
            *position += 1;
            true
        }
        Some(b'+') => {
            *position += 1;
            false
        }
        _ => false,
    };
    let start = *position;
    let mut magnitude = 0_i64;
    while bytes.get(*position).is_some_and(u8::is_ascii_digit) {
        magnitude = magnitude
            .saturating_mul(10)
            .saturating_add(i64::from(bytes[*position] - b'0'));
        *position += 1;
    }
    (start != *position).then_some(if is_negative { -magnitude } else { magnitude })
}

fn parse_int_literal(value: &str, field: &str, offset: usize) -> Result<i64, QueryError> {
    value
        .parse()
        .map_err(|error: ParseIntError| match error.kind() {
            IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => {
                QueryError::new(offset, format!("INT literal out of range for {field}"))
            }
            _ => QueryError::new(
                offset,
                format!("invalid INT literal for {field}: {value:?} is not a whole number"),
            ),
        })
}

fn parse_double_literal(value: &str, field: &str, offset: usize) -> Result<f64, QueryError> {
    let number: f64 = if is_hex_float(value) {
        match parse_hex_float(value) {
            Some(number) => number,
            None => {
                return Err(invalid_numeric_literal(
                    "DOUBLE",
                    value,
                    field,
                    offset,
                    "decimal number",
                ));
            }
        }
    } else {
        match value.parse() {
            Ok(number) => number,
            Err(_) => {
                return Err(invalid_numeric_literal(
                    "DOUBLE",
                    value,
                    field,
                    offset,
                    "decimal number",
                ));
            }
        }
    };
    if number.is_finite() {
        Ok(number)
    } else if value.bytes().any(|byte| byte.is_ascii_digit()) {
        Err(QueryError::new(
            offset,
            format!("DOUBLE literal out of range for {field}"),
        ))
    } else {
        // Rust parses NaN and infinity names, which no DOUBLE value can match.
        Err(invalid_numeric_literal(
            "DOUBLE",
            value,
            field,
            offset,
            "decimal number",
        ))
    }
}

fn is_hex_float(value: &str) -> bool {
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    unsigned.starts_with("0x") || unsigned.starts_with("0X")
}

fn parse_hex_float(value: &str) -> Option<f64> {
    let bytes = value.as_bytes();
    let mut position = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let is_negative = bytes.first() == Some(&b'-');
    position += 2;
    let mut bits = Vec::new();
    let mut fraction_digits = 0_i64;
    let mut has_point = false;
    let mut digit_count = 0;
    while let Some(&byte) = bytes.get(position) {
        if byte == b'.' && !has_point {
            has_point = true;
            position += 1;
            continue;
        }
        let Some(nibble) = (byte as char).to_digit(16) else {
            break;
        };
        digit_count += 1;
        if has_point {
            fraction_digits += 1;
        }
        for shift in (0..4).rev() {
            bits.push(((nibble >> shift) & 1) as u8);
        }
        position += 1;
    }
    if digit_count == 0 || !matches!(bytes.get(position), Some(b'p' | b'P')) {
        return None;
    }
    position += 1;
    let binary_exponent = parse_signed_exponent(bytes, &mut position)?;
    if position != bytes.len() {
        return None;
    }
    let sign_bits = u64::from(is_negative) << 63;
    let Some(first_one) = bits.iter().position(|bit| *bit == 1) else {
        return Some(f64::from_bits(sign_bits));
    };
    let significant_bits = &bits[first_one..];
    let highest_exponent = binary_exponent
        .saturating_sub(fraction_digits.saturating_mul(4))
        .saturating_add(i64::try_from(bits.len() - first_one - 1).expect("query length fits i64"));
    if highest_exponent > 1023 {
        return Some(f64::INFINITY);
    }
    if highest_exponent < -1075 {
        return Some(f64::from_bits(sign_bits));
    }
    if highest_exponent >= -1022 {
        let mut significand = round_binary_significand(significant_bits, 53);
        let mut exponent = highest_exponent;
        if significand == (1 << 53) {
            significand >>= 1;
            exponent += 1;
        }
        if exponent > 1023 {
            return Some(f64::INFINITY);
        }
        let exponent_bits = u64::try_from(exponent + 1023).expect("normal exponent is positive");
        return Some(f64::from_bits(
            sign_bits | (exponent_bits << 52) | (significand & ((1 << 52) - 1)),
        ));
    }
    let keep_count = usize::try_from(highest_exponent + 1075).expect("subnormal bit count");
    let significand = round_binary_significand(significant_bits, keep_count);
    Some(f64::from_bits(sign_bits | significand))
}

fn round_binary_significand(bits: &[u8], keep_count: usize) -> u64 {
    let mut significand = 0_u64;
    for index in 0..keep_count {
        significand = (significand << 1) | u64::from(bits.get(index).copied().unwrap_or(0));
    }
    if bits.get(keep_count) == Some(&1)
        && (significand & 1 == 1 || bits[keep_count + 1..].contains(&1))
    {
        significand += 1;
    }
    significand
}

fn invalid_numeric_literal(
    index_type: &str,
    value: &str,
    field: &str,
    offset: usize,
    syntax: &str,
) -> QueryError {
    QueryError::new(
        offset,
        format!("invalid {index_type} literal for {field}: {value:?} is not a {syntax}"),
    )
}

fn describe(kind: &TokenKind) -> String {
    match kind {
        TokenKind::Identifier(value) => format!("identifier {value:?}"),
        TokenKind::QuotedIdentifier(value) => format!("quoted identifier {value:?}"),
        TokenKind::String(_) => "string literal".to_owned(),
        TokenKind::Number(value) => format!("number {value:?}"),
        TokenKind::Symbol(value) => format!("symbol {value:?}"),
        TokenKind::End => "end of query".to_owned(),
    }
}

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
        Ok(Expression::Predicate(Predicate {
            field,
            index_type,
            operator,
            literals,
        }))
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
    ) -> Result<Literal, QueryError> {
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
                Literal::Int(parse_int_literal(value, field, token.offset)?)
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
        Ok(literal)
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
    let invalid = || {
        QueryError::new(
            offset,
            format!("invalid DOUBLE literal for {field}: {value:?} is not a decimal number"),
        )
    };
    let number: f64 = value.parse().map_err(|_| invalid())?;
    if number.is_finite() {
        Ok(number)
    } else if value.bytes().any(|byte| byte.is_ascii_digit()) {
        Err(QueryError::new(
            offset,
            format!("DOUBLE literal out of range for {field}"),
        ))
    } else {
        // Rust parses NaN and infinity names, which no DOUBLE value can match.
        Err(invalid())
    }
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

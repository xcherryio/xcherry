// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use crate::QueryError;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TokenKind {
    Identifier(String),
    String(String),
    Number(String),
    Symbol(&'static str),
    End,
}

#[derive(Debug, Clone)]
pub(crate) struct Token {
    pub kind: TokenKind,
    pub offset: usize,
}

pub(crate) fn lex(input: &str) -> Result<Vec<Token>, QueryError> {
    let mut lexer = Lexer { input, offset: 0 };
    let mut tokens = Vec::new();
    while let Some(token) = lexer.next_token()? {
        tokens.push(token);
    }
    tokens.push(Token {
        kind: TokenKind::End,
        offset: input.len(),
    });
    Ok(tokens)
}

struct Lexer<'a> {
    input: &'a str,
    offset: usize,
}

impl Lexer<'_> {
    fn next_token(&mut self) -> Result<Option<Token>, QueryError> {
        while self.peek().is_some_and(char::is_whitespace) {
            self.advance();
        }
        let start = self.offset;
        let Some(character) = self.peek() else {
            return Ok(None);
        };
        let kind = match character {
            '\'' | '"' | '`' => self.quoted(character, start)?,
            '(' => self.single_symbol("("),
            ')' => self.single_symbol(")"),
            ',' => self.single_symbol(","),
            '=' => self.single_symbol("="),
            '>' => {
                self.advance();
                if self.take('=') {
                    TokenKind::Symbol(">=")
                } else {
                    TokenKind::Symbol(">")
                }
            }
            '<' => {
                self.advance();
                if self.take('=') {
                    TokenKind::Symbol("<=")
                } else if self.take('>') {
                    TokenKind::Symbol("<>")
                } else {
                    TokenKind::Symbol("<")
                }
            }
            '!' => {
                self.advance();
                if self.take('=') {
                    TokenKind::Symbol("!=")
                } else {
                    return Err(QueryError::new(
                        start,
                        "unsupported operator !; expected !=",
                    ));
                }
            }
            '-' | '0'..='9' => self.number(start)?,
            _ if character.is_alphabetic() || character == '_' => self.identifier(),
            _ => {
                return Err(QueryError::new(
                    start,
                    format!("unsupported character {character:?}"),
                ));
            }
        };
        Ok(Some(Token {
            kind,
            offset: start,
        }))
    }

    fn quoted(&mut self, delimiter: char, start: usize) -> Result<TokenKind, QueryError> {
        self.advance();
        let mut value = String::new();
        while let Some(character) = self.advance() {
            if character == delimiter {
                if self.take(delimiter) {
                    value.push(delimiter);
                    continue;
                }
                return Ok(if delimiter == '`' {
                    TokenKind::Identifier(value)
                } else {
                    TokenKind::String(value)
                });
            }
            if character == '\\' {
                let Some(escaped) = self.advance() else {
                    return Err(QueryError::new(
                        start,
                        "unterminated escape in quoted value",
                    ));
                };
                value.push(match escaped {
                    '\\' => '\\',
                    '\'' => '\'',
                    '"' => '"',
                    '`' => '`',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    _ => {
                        return Err(QueryError::new(
                            self.offset - escaped.len_utf8() - 1,
                            format!("unsupported escape \\{escaped}"),
                        ));
                    }
                });
            } else {
                value.push(character);
            }
        }
        Err(QueryError::new(start, "unterminated quoted value"))
    }

    fn number(&mut self, start: usize) -> Result<TokenKind, QueryError> {
        self.take('-');
        let digits_start = self.offset;
        while self
            .peek()
            .is_some_and(|character| character.is_ascii_digit())
        {
            self.advance();
        }
        if digits_start == self.offset {
            return Err(QueryError::new(start, "expected digits after minus sign"));
        }
        if self.take('.') {
            let decimal_start = self.offset;
            while self
                .peek()
                .is_some_and(|character| character.is_ascii_digit())
            {
                self.advance();
            }
            if decimal_start == self.offset {
                return Err(QueryError::new(
                    start,
                    "expected digits after decimal point",
                ));
            }
        }
        if self.take('e') || self.take('E') {
            if !self.take('+') {
                self.take('-');
            }
            let exponent_start = self.offset;
            while self
                .peek()
                .is_some_and(|character| character.is_ascii_digit())
            {
                self.advance();
            }
            if exponent_start == self.offset {
                return Err(QueryError::new(
                    start,
                    "expected digits in numeric exponent",
                ));
            }
        }
        Ok(TokenKind::Number(self.input[start..self.offset].to_owned()))
    }

    fn identifier(&mut self) -> TokenKind {
        let start = self.offset;
        while self
            .peek()
            .is_some_and(|character| character.is_alphanumeric() || character == '_')
        {
            self.advance();
        }
        TokenKind::Identifier(self.input[start..self.offset].to_owned())
    }

    fn single_symbol(&mut self, symbol: &'static str) -> TokenKind {
        self.advance();
        TokenKind::Symbol(symbol)
    }

    fn take(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.offset..].chars().next()
    }

    fn advance(&mut self) -> Option<char> {
        let character = self.peek()?;
        self.offset += character.len_utf8();
        Some(character)
    }
}

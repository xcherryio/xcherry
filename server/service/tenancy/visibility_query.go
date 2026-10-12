// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/superdurable/dex/gen/dexpb"
)

const (
	// SearchAttributeDexTenantID is the Keyword search attribute that holds a Flow's tenant.
	SearchAttributeDexTenantID = "DexTenantID"
	workflowIDSearchAttribute  = "WorkflowId"
	parentFlowIDAttribute      = "DexParentFlowID"
)

// TenantVisibilityQuery parses query with the visibility grammar, maps Flow ID literals to stored IDs, and returns a
// new query that matches only the tenant's Flows. extraClause, when not empty, is ANDed as well. Any construct
// outside comparisons, IN, BETWEEN, STARTS_WITH, IS NULL, AND, OR, NOT, parentheses, ORDER BY and literals is refused.
func TenantVisibilityQuery(tenantID string, query string, extraClause string) (string, error) {
	parsed, err := parseVisibilityQuery(query)
	if err != nil {
		return "", err
	}
	if err := parsed.mapFlowIDLiterals(tenantID); err != nil {
		return "", err
	}
	clauses := []string{}
	if parsed.condition != nil {
		clauses = append(clauses, "("+parsed.condition.serialize()+")")
	}
	clauses = append(clauses, SearchAttributeDexTenantID+" = "+quoteVisibilityString(tenantID))
	if extraClause != "" {
		clauses = append(clauses, "("+extraClause+")")
	}
	serialized := strings.Join(clauses, " AND ")
	if len(parsed.orderBy) > 0 {
		serialized += " ORDER BY " + strings.Join(parsed.orderBy, ", ")
	}
	return serialized, nil
}

type visibilityQuery struct {
	condition visibilityExpression
	orderBy   []string
}

type visibilityExpression interface {
	serialize() string
	mapFlowIDLiterals(tenantID string) error
}

type visibilityBinaryExpression struct {
	operator    string
	left, right visibilityExpression
}

type visibilityNotExpression struct {
	operand visibilityExpression
}

// visibilityPredicate is one attribute test, such as `A = 1`, `A NOT IN (1, 2)` or `A IS NOT NULL`.
type visibilityPredicate struct {
	attribute string
	operator  string
	values    []visibilityLiteral
}

type visibilityLiteral struct {
	isString bool
	text     string
}

func (q *visibilityQuery) mapFlowIDLiterals(tenantID string) error {
	for _, order := range q.orderBy {
		if strings.EqualFold(strings.Fields(order)[0], SearchAttributeDexTenantID) {
			return fmt.Errorf("query must not reference %s", SearchAttributeDexTenantID)
		}
	}
	if q.condition == nil {
		return nil
	}
	return q.condition.mapFlowIDLiterals(tenantID)
}

func (e *visibilityBinaryExpression) serialize() string {
	return "(" + e.left.serialize() + ") " + e.operator + " (" + e.right.serialize() + ")"
}

func (e *visibilityBinaryExpression) mapFlowIDLiterals(tenantID string) error {
	if err := e.left.mapFlowIDLiterals(tenantID); err != nil {
		return err
	}
	return e.right.mapFlowIDLiterals(tenantID)
}

func (e *visibilityNotExpression) serialize() string {
	return "NOT (" + e.operand.serialize() + ")"
}

func (e *visibilityNotExpression) mapFlowIDLiterals(tenantID string) error {
	return e.operand.mapFlowIDLiterals(tenantID)
}

func (p *visibilityPredicate) serialize() string {
	values := make([]string, len(p.values))
	for i, value := range p.values {
		values[i] = value.serialize()
	}
	switch p.operator {
	case "IN", "NOT IN":
		return p.attribute + " " + p.operator + " (" + strings.Join(values, ", ") + ")"
	case "BETWEEN", "NOT BETWEEN":
		return p.attribute + " " + p.operator + " " + values[0] + " AND " + values[1]
	case "IS NULL", "IS NOT NULL":
		return p.attribute + " " + p.operator
	default:
		return p.attribute + " " + p.operator + " " + values[0]
	}
}

func (p *visibilityPredicate) mapFlowIDLiterals(tenantID string) error {
	switch {
	case strings.EqualFold(p.attribute, SearchAttributeDexTenantID):
		return fmt.Errorf("query must not reference %s", SearchAttributeDexTenantID)
	case strings.EqualFold(p.attribute, workflowIDSearchAttribute), strings.EqualFold(p.attribute, parentFlowIDAttribute):
		for i := range p.values {
			if !p.values[i].isString {
				return fmt.Errorf("%s must be compared with strings", p.attribute)
			}
			p.values[i].text = StoredFlowID(tenantID, p.values[i].text)
		}
	}
	return nil
}

func (l visibilityLiteral) serialize() string {
	if l.isString {
		return quoteVisibilityString(l.text)
	}
	return l.text
}

func quoteVisibilityString(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`
}

// visibilityQueryParser is a recursive-descent parser over the tokens of one query.
type visibilityQueryParser struct {
	tokens   []visibilityToken
	position int
}

type visibilityTokenKind int

const (
	visibilityTokenEnd visibilityTokenKind = iota
	visibilityTokenWord
	visibilityTokenIdentifier
	visibilityTokenString
	visibilityTokenNumber
	visibilityTokenSymbol
)

type visibilityToken struct {
	kind visibilityTokenKind
	text string
}

func parseVisibilityQuery(query string) (*visibilityQuery, error) {
	tokens, err := tokenizeVisibilityQuery(query)
	if err != nil {
		return nil, err
	}
	parser := &visibilityQueryParser{tokens: tokens}
	parsed := &visibilityQuery{}
	if !parser.isKeyword("ORDER") && parser.peek().kind != visibilityTokenEnd {
		if parsed.condition, err = parser.parseOr(); err != nil {
			return nil, err
		}
	}
	if parser.acceptKeyword("ORDER") {
		if !parser.acceptKeyword("BY") {
			return nil, fmt.Errorf("ORDER must be followed by BY")
		}
		for {
			attribute, err := parser.parseAttribute()
			if err != nil {
				return nil, err
			}
			order := attribute
			if parser.isKeyword("ASC") || parser.isKeyword("DESC") {
				order += " " + strings.ToUpper(parser.next().text)
			}
			parsed.orderBy = append(parsed.orderBy, order)
			if !parser.acceptSymbol(",") {
				break
			}
		}
	}
	if token := parser.peek(); token.kind != visibilityTokenEnd {
		return nil, fmt.Errorf("unsupported query construct near %q", token.text)
	}
	return parsed, nil
}

func (p *visibilityQueryParser) parseOr() (visibilityExpression, error) {
	left, err := p.parseAnd()
	for err == nil && p.acceptKeyword("OR") {
		var right visibilityExpression
		right, err = p.parseAnd()
		left = &visibilityBinaryExpression{operator: "OR", left: left, right: right}
	}
	return left, err
}

func (p *visibilityQueryParser) parseAnd() (visibilityExpression, error) {
	left, err := p.parseNot()
	for err == nil && p.acceptKeyword("AND") {
		var right visibilityExpression
		right, err = p.parseNot()
		left = &visibilityBinaryExpression{operator: "AND", left: left, right: right}
	}
	return left, err
}

func (p *visibilityQueryParser) parseNot() (visibilityExpression, error) {
	if p.acceptKeyword("NOT") {
		operand, err := p.parseNot()
		return &visibilityNotExpression{operand: operand}, err
	}
	if p.acceptSymbol("(") {
		expression, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if !p.acceptSymbol(")") {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		return expression, nil
	}
	return p.parsePredicate()
}

func (p *visibilityQueryParser) parsePredicate() (visibilityExpression, error) {
	attribute, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}
	predicate := &visibilityPredicate{attribute: attribute}
	if token := p.peek(); token.kind == visibilityTokenSymbol && isComparisonOperator(token.text) {
		predicate.operator = p.next().text
		return predicate, p.appendLiterals(predicate, 1)
	}
	negation := ""
	if p.acceptKeyword("NOT") {
		negation = "NOT "
	}
	switch {
	case p.acceptKeyword("IN"):
		predicate.operator = negation + "IN"
		if !p.acceptSymbol("(") {
			return nil, fmt.Errorf("IN must be followed by a parenthesized list")
		}
		for {
			if err := p.appendLiterals(predicate, 1); err != nil {
				return nil, err
			}
			if !p.acceptSymbol(",") {
				break
			}
		}
		if !p.acceptSymbol(")") {
			return nil, fmt.Errorf("missing closing parenthesis after IN list")
		}
	case p.acceptKeyword("BETWEEN"):
		predicate.operator = negation + "BETWEEN"
		if err := p.appendLiterals(predicate, 1); err != nil {
			return nil, err
		}
		if !p.acceptKeyword("AND") {
			return nil, fmt.Errorf("BETWEEN requires AND")
		}
		if err := p.appendLiterals(predicate, 1); err != nil {
			return nil, err
		}
	case p.acceptKeyword("STARTS_WITH"):
		predicate.operator = negation + "STARTS_WITH"
		if err := p.appendLiterals(predicate, 1); err != nil {
			return nil, err
		}
	case negation == "" && p.acceptKeyword("IS"):
		predicate.operator = "IS NULL"
		if p.acceptKeyword("NOT") {
			predicate.operator = "IS NOT NULL"
		}
		if !p.acceptKeyword("NULL") {
			return nil, fmt.Errorf("IS must be followed by NULL or NOT NULL")
		}
	default:
		return nil, fmt.Errorf("unsupported query construct after %q", attribute)
	}
	return predicate, nil
}

func (p *visibilityQueryParser) parseAttribute() (string, error) {
	token := p.next()
	switch token.kind {
	case visibilityTokenIdentifier:
		return token.text, nil
	case visibilityTokenWord:
		if !isVisibilityKeyword(token.text) {
			return token.text, nil
		}
	}
	return "", fmt.Errorf("expected an attribute name, found %q", token.text)
}

func (p *visibilityQueryParser) appendLiterals(predicate *visibilityPredicate, count int) error {
	for i := 0; i < count; i++ {
		token := p.next()
		switch {
		case token.kind == visibilityTokenString:
			predicate.values = append(predicate.values, visibilityLiteral{isString: true, text: token.text})
		case token.kind == visibilityTokenNumber:
			predicate.values = append(predicate.values, visibilityLiteral{text: token.text})
		case token.kind == visibilityTokenWord && (strings.EqualFold(token.text, "true") ||
			strings.EqualFold(token.text, "false")):
			predicate.values = append(predicate.values, visibilityLiteral{text: strings.ToLower(token.text)})
		default:
			return fmt.Errorf("expected a literal, found %q", token.text)
		}
	}
	return nil
}

func (p *visibilityQueryParser) peek() visibilityToken {
	return p.tokens[p.position]
}

func (p *visibilityQueryParser) next() visibilityToken {
	token := p.tokens[p.position]
	if token.kind != visibilityTokenEnd {
		p.position++
	}
	return token
}

func (p *visibilityQueryParser) isKeyword(keyword string) bool {
	token := p.peek()
	return token.kind == visibilityTokenWord && strings.EqualFold(token.text, keyword)
}

func (p *visibilityQueryParser) acceptKeyword(keyword string) bool {
	if !p.isKeyword(keyword) {
		return false
	}
	p.position++
	return true
}

func (p *visibilityQueryParser) acceptSymbol(symbol string) bool {
	token := p.peek()
	if token.kind != visibilityTokenSymbol || token.text != symbol {
		return false
	}
	p.position++
	return true
}

func isComparisonOperator(symbol string) bool {
	switch symbol {
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return true
	}
	return false
}

func isVisibilityKeyword(word string) bool {
	switch strings.ToUpper(word) {
	case "AND", "OR", "NOT", "IN", "BETWEEN", "STARTS_WITH", "IS", "NULL", "ORDER", "BY", "ASC", "DESC",
		"TRUE", "FALSE":
		return true
	}
	return false
}

func tokenizeVisibilityQuery(query string) ([]visibilityToken, error) {
	var tokens []visibilityToken
	runes := []rune(query)
	for i := 0; i < len(runes); {
		current := runes[i]
		switch {
		case unicode.IsSpace(current):
			i++
		case current == '\'' || current == '"':
			text, next, err := readQuotedVisibilityString(runes, i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, visibilityToken{kind: visibilityTokenString, text: text})
			i = next
		case current == '`':
			end := i + 1
			for end < len(runes) && runes[end] != '`' {
				end++
			}
			if end == len(runes) || end == i+1 {
				return nil, fmt.Errorf("invalid quoted attribute name")
			}
			name := string(runes[i+1 : end])
			if !isPlainVisibilityName(name) {
				return nil, fmt.Errorf("unsupported attribute name %q", name)
			}
			tokens = append(tokens, visibilityToken{kind: visibilityTokenIdentifier, text: name})
			i = end + 1
		case current == '_' || unicode.IsLetter(current):
			end := i
			for end < len(runes) && (runes[end] == '_' || unicode.IsLetter(runes[end]) || unicode.IsDigit(runes[end])) {
				end++
			}
			tokens = append(tokens, visibilityToken{kind: visibilityTokenWord, text: string(runes[i:end])})
			i = end
		case unicode.IsDigit(current) || (current == '-' && i+1 < len(runes) && unicode.IsDigit(runes[i+1])):
			end := i + 1
			for end < len(runes) && (unicode.IsDigit(runes[end]) || runes[end] == '.') {
				end++
			}
			number := string(runes[i:end])
			if _, err := strconv.ParseFloat(number, 64); err != nil {
				return nil, fmt.Errorf("invalid number %q", number)
			}
			tokens = append(tokens, visibilityToken{kind: visibilityTokenNumber, text: number})
			i = end
		default:
			symbol, ok := readVisibilitySymbol(runes, i)
			if !ok {
				return nil, fmt.Errorf("unsupported character %q in query", current)
			}
			tokens = append(tokens, visibilityToken{kind: visibilityTokenSymbol, text: symbol})
			i += len([]rune(symbol))
		}
	}
	return append(tokens, visibilityToken{kind: visibilityTokenEnd}), nil
}

func readQuotedVisibilityString(runes []rune, start int) (string, int, error) {
	quote := runes[start]
	var text strings.Builder
	for i := start + 1; i < len(runes); i++ {
		switch runes[i] {
		case '\\':
			if i+1 == len(runes) {
				return "", 0, fmt.Errorf("unterminated string literal")
			}
			i++
			text.WriteRune(runes[i])
		case quote:
			if i+1 < len(runes) && runes[i+1] == quote {
				text.WriteRune(quote)
				i++
				continue
			}
			return text.String(), i + 1, nil
		default:
			text.WriteRune(runes[i])
		}
	}
	return "", 0, fmt.Errorf("unterminated string literal")
}

func readVisibilitySymbol(runes []rune, start int) (string, bool) {
	if start+1 < len(runes) {
		pair := string(runes[start : start+2])
		switch pair {
		case "!=", "<>", "<=", ">=":
			return pair, true
		}
	}
	switch runes[start] {
	case '=', '<', '>', '(', ')', ',':
		return string(runes[start]), true
	}
	return "", false
}

func isPlainVisibilityName(name string) bool {
	for _, character := range name {
		if character != '_' && !unicode.IsLetter(character) && !unicode.IsDigit(character) {
			return false
		}
	}
	return true
}

// ValidateTenantAttributeWrites refuses a tenant's indexed Attribute write that would set the tenant search attribute.
func ValidateTenantAttributeWrites(tenantID string, writes []*dexpb.AttributeWrite) error {
	if tenantID == "" {
		return nil
	}
	for _, write := range writes {
		if write.GetIndexConfig().GetEnable() && strings.EqualFold(write.GetKey(), SearchAttributeDexTenantID) {
			return fmt.Errorf("attribute key %s is reserved for the tenant index", SearchAttributeDexTenantID)
		}
	}
	return nil
}

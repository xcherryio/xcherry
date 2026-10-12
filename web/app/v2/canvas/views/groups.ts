// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/**
 * SEMANTIC GROUPS — a labelled background region saying "these steps are the same part of the flow",
 * in the spirit of n8n's coloured group boxes ("Triggers", "Data Prep", "Google Maps API").
 *
 * THESE LABELS ARE MOCKED, AND THAT IS THE POINT. n8n's groups are hand-authored: a person typed
 * "Data Prep" and drew the box. Nothing in the Flow Definition Graph carries an authorial grouping, so
 * for the POC these are model-authored per fixture and marked as such on screen. In the product they
 * come from the code — see BACKEND_CONTRACT.md §3.4.
 *
 * WHY MOCKED RATHER THAN DERIVED. Five derivations were measured across the four fixtures first, and
 * every one of them is too weak to carry the feature:
 *
 *   | method                  | ai-agent       | job-post | money-transfer | order-proc |
 *   |-------------------------|----------------|----------|----------------|------------|
 *   | strongly-connected      | ONE group of 8 | none     | none           | none       |
 *   | cut vertices            | 8 of 9         | 2 of 3   | 4 of 6         | 1 of 3     |
 *   | declaration proximity   | 1 group of 9   | 1 of 3   | 1 of 6         | 1 of 3     |
 *   | shared name word        | Tool(3)        | 2        | 2 + 2          | none       |
 *   | actor                   | 5/3/1          | 1/2      | 6              | 2/1        |
 *
 * SCC and cut vertices are degenerate — the agent flow is one big cycle, so "the loop" is 8 of its 9
 * steps and nearly every step is an articulation point. Declaration proximity finds nothing because
 * Dex steps are declared contiguously; there is no paragraph structure in the source to recover.
 * Actor is a complete partition but the card's colour already carries it. Shared name word was the
 * best of them and still yielded at most one useful group per flow, and none at all for
 * order-processing.
 *
 * The measurement is kept here because it is the argument for the backend ask: grouping needs
 * authorial intent, and authorial intent is not in the wire.
 *
 * One thing the derivation DID establish, worth keeping when the real source arrives: the useful
 * tiebreak is flow distance. Bucketing money-transfer by leading word gives `Create` =
 * {CreateDebitMemo, CreateCreditMemo} — a grouping by verb, two steps doing the same kind of thing at
 * different points. Preferring members that sit close together in flow order gives the debit side and
 * the credit side instead, which is how a person actually draws the boxes.
 */

export interface StepGroup {
  id: string
  /** Shown top-left inside the band, like n8n. Short — it is a region name, not a sentence. */
  label: string
  /** Why these belong together. Sidebar only, never on the canvas. */
  reason: string
  /** StepTypes, not ids — the mock is written against the source vocabulary, not our id scheme. */
  stepTypes: string[]  /** Member Step ids, which the canvas lays out by. */
  stepIds: string[]
}

/**
 * THE TABLE ITSELF LIVES IN THE FIXTURE LAYER, and this file deliberately no longer holds it.
 *
 * It used to, and that made `src/canvas/` — shipping canvas code — the home of hand-authored per-flow content
 * naming a domain's Step types and writing a domain's prose. A second reader falsified a claim in the
 * interface finding with exactly that: the refund bands were the counterexample to "no refund-specific
 * vocabulary in shipping code outside the fixture layer", and they were the bands the finding credits with
 * making the guardrail visible.
 *
 * So the authored regions are in `@/adapters/dex/fixtures/groups`, beside the charters, the run history and
 * the field tables — every other piece of per-flow content that cannot be derived from a graph. What is left
 * here is the SHAPE of a group and the record of why grouping cannot be derived, which are claims about the
 * canvas rather than about any flow.
 *
 * The resolution functions moved with the table, because they only mean anything applied to it.
 */

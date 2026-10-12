# Canvas layout: principles, not per-screenshot tuning

A view here ranks the graph, chooses which transitions are lines, and hands placement and line routing
to ELK's layered algorithm (`elkPlacement.ts`). Every rule and spacing constant is therefore a claim
about every Flow, not just the one on screen. This directory holds the machinery that keeps that claim
honest.

## The loop

Change a layout constant or rule the same way every time:

1. **Measure first.** `layoutQuality.test.ts` runs all shipped Flow definitions in
   `docs/src/data/flow-definitions/` Collapsed and Expanded, in both directions. Run it before touching
   anything and keep the numbers.
2. **Find the worst case by number, not by eye.** The report names the flow and mode holding each
   worst metric. That is where to look, and it is often not the drawing you were shown.
3. **Change the rule, then re-measure the whole corpus.** A constant that improves one graph and
   worsens four is a regression the screenshot cannot show you.
4. **Break the guardrail on purpose.** Revert the fix and confirm the test fails. A guardrail that
   passes both ways guards nothing — three attempts at one in this directory were vacuous before
   anyone noticed.
5. **Set the limit from the gap between the two runs.** With the merge guard removed, the region-hole
   metric reads 1.09; an ordinary gap between fan members reads 0.65. The limit sits at 0.85 because
   the data put it there.

## The principles

Declared in `layoutQuality.ts` with their metric and limit, so each one can fail a test rather than
only guide a reader. Current corpus worst is in the report the test prints.

| Principle | Reading |
| --- | --- |
| `no-collision` | Two cards never occupy the same space. |
| `tight-regions` | A region has no gap inside it wide enough to hold a card. |
| `group-cohesion` | A group sits in one place where the graph allows it. |
| `short-edges` | An arrow spans about a rank, not the whole drawing. |
| `reading-order` | The first card is the start of the Flow. |
| `clear-lines` | No line passes under a card it does not join. |

They pull against each other on purpose. Cohesion moves a card toward its group and lengthens its
edges; `short-edges` is where that stops. Do not relax a limit to let a change through without saying
which principle you are trading away and what the corpus number becomes.

`short-edges` was raised from 6.5 to 7 when an open stage became one ELK container, so its grey band
is a single region. That trades edge length for `tight-regions` and `group-cohesion`: the corpus worst
edge span went from 5.60 to 6.86 (`customer-refund-agentic.json`, Expanded, top-down, where lines from
across the Flow enter an 11-Step stage), and the worst region hole went from 0.548 to 0.323.
`clear-lines` reads 0 across the corpus; forcing straight lines reads 38.

## How a Flow is drawn

In priority order; a lower rule never breaks a higher one.

1. Nothing is lost. Every Step of the main story and every ordinary transition appears, as a line or
   as a ↪ chip on the card it leaves. Failure handling waits for a selection and is counted on the
   Step cards it leaves (see Failure handling below). A wait the analyzer could not name is drawn as a wait, never as
   automatic.
2. No line passes under a card (ELK's orthogonal routing; `clear-lines`).
3. The start comes first and is on screen when the canvas opens.
4. Status is never colour alone: every state has words or a glyph.
5. Layout never blanks the canvas: each failed ELK pass falls back to a simpler one, the last to a
   plain grid, and the legend says so.
6. One line in (`mainLine.ts`): each Step is drawn with the transition from the latest Step that leads
   to it, from its own group when one does; every other transition is a ↪ chip. Main lines are chosen
   once between Steps and every view draws the ones that cross between its cards, so opening a stage
   never turns a line into a chip or back. Cards are ranked by the drawn lines only.
7. Side to side (`sideLanes.ts`): a line that loops around both of its cards, or runs long, leaves the
   side of its card along a clear lane when that is shorter and crosses no more lines.
8. A long Flow is drawn long, never folded into columns. One too large to read whole opens on its
   start at a readable zoom (`READABLE_ZOOM`).
9. An open stage is one region: its Steps are one ELK container. If ELK cannot place the containers,
   groups fall back to bands that hug their members (`groupBands.ts`) and never enclose another card.
10. Attach, don't connect: the Actions that answer a waiting Step, the RPC that starts it, the child
    Flow it launches and how it ends the Flow are chips on its card, not boxes with lines.

## Stages

A group of two or more Steps that forms one run (its Steps lead to each other) or one choice (a single
Step leads into each of its parts, like an agent's tools) is a stage (`groupShape.ts`): closed, it is one
card (`stageCard.ts`) with ⊕; open, its Steps sit in one labelled band with ⊖. Any other group always
shows its Steps in its band and never folds, because opening it would scatter unrelated pieces; the
analyzer's `v2_step_group_shape` warning names those pieces so the author can split the group. Both
judge the main story only: a failure-handling Step belongs to no stage, and a failure transition joins
nothing. Collapsed closes every stage and Expanded opens every one; both start
the view again. A one-Step group is always drawn as its Step in its band. Opening or closing one stage
keeps the reader's place (`anchor.ts`, `../useStageScene.ts`): the new Scene is moved so the stage's
leading edge stays where it was. When a run is shown, the stage holding the waiting Step opens.

## The main line

A Step's main line comes from the source ranked closest before it (`longestPathRanks`), so a chain reads
straight, a shared finish is drawn from the end of the work, and a shortcut is a chip. A source in the
Step's own group wins, then reading order, then source order. Failure transitions are not in the drawing.
The decoder sorts transitions by source position, because the analyzer lists edges by node id. The
choice therefore does not change with the edge order on the wire, nor when code moves down the file.

## Regions only exist for FDG 2.0

`dex:group` is an FDG 2.0 directive, so a 1.0 definition produces no regions and passes
`tight-regions` and `group-cohesion` vacuously. Three of the shipped definitions carry groups today, so
those two principles are exercised by 12 of 128 scenes. Do not read the corpus count as their coverage.
The generation side needs nothing: the analyzer already errors unless every Step declares exactly one
`dex:group`, and rejects one group id used with two labels, so membership is total and unambiguous.

Porting a second FDG 2.0 Flow is the way to widen that, not loosening a limit.

## Metric traps found the hard way

- **Area is not tightness.** Five failure Steps at five ranks make a tall thin region that is 81%
  floor and entirely correct. Judging a region by how much of its area holds a card punished exactly
  the vertical strip a group is supposed to form.
- **Do not measure the band, measure the cards.** Uncovered-width charged every region for its own
  padding and label strip: 0.435 of a single-card band left-right, with no defect present.
- **Express a gap in cards, not pixels.** A hole reads as a hole because a missing box would fit. The
  same 240px means different things beside a 260px card and a 60px one.
- **Both axes exist.** Ranks run down the page top-down and across it left-right. Two metrics here
  assumed one axis and reported nonsense for half the corpus, including a 650 where the true value
  was 5.3.
- **Check the edge set reaches the function.** `cyclicRanks` takes `{from, to}`; `PocFlow`
  transitions carry `fromStepId`/`toStepId`. Passing them unmapped silently dropped every edge and
  returned rank 0 for all 25 Steps, which made an adjacency test vacuously true and moved cards that
  should not have moved.

## Bugs this machinery has caught

- **Region merging under-converged.** The greedy merge bounded its passes by the live `rects.length`,
  which shrinks on every merge while the counter rises. A group spanning seven rows stopped after
  four merges and shipped three regions where one was available.
- **A stylesheet import order made `stroke-width` dead.** `@xyflow/react`'s stylesheet loads after
  `canvas.css` at equal specificity, so an unqualified `.react-flow__edge-path` rule never applied and
  every edge sat at the library default.
- **`nodesep`/`ranksep` are inert here.** This layout takes only within-rank order from dagre.
  Setting them looks like a spacing fix and changes nothing; `rankBase` is the real lever.
## Failure handling

The canvas draws the main story first: every Step the Flow reaches on purpose, however rarely,
including business rejections, failed checks and Steps an RPC moves the Flow to. Failure handling is
every Step that only failure transitions reach, directly or through other such Steps
(`failureHandling` in `../model/pocFlow.ts`). A Step that catches its own error and chooses where to go
makes an ordinary decision and stays in the main story; the analyzer judges groups on the same split.

The layout leaves failure handling and every failure transition out (`mainStory`). A Step card that a
failure leaves carries ⚠ and the number of its failure transitions. A stage card carries none, so the
collapsed view stays quiet, and the controls bar says nothing about failure handling; the legend notes how
many Steps it leaves out. Collapsed and Expanded never show failure-handling Steps.

Selecting a card with ⚠ draws its detour beside it (`failureDetours.ts`): the Steps its failures lead
to, dashed, in a clear lane to its right (below it, left-right), and a pointer for a failure that goes
straight to a main-story Step drawn elsewhere. The way back is a ↩ chip. Nothing already drawn moves:
the detour slides along the card's side, or out past the cards level with it, until it covers no card,
and the view pans just enough to show it beside the Step panel, which never refits the view. Selecting a
Step inside the detour keeps it; selecting anything else closes it. A run that went through failure
handling draws that detour without a click, from the Step that failed into it.

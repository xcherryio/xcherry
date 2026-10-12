# Legacy and third-party notices

The Sustainable Use License 1.0 applies only to material owned or
licensable by Super Durable, Inc. It does not replace the licenses below.

## Legacy cutoff

The complete repository snapshot at commit
`1a61670238cbdc8f2a0a6dd78cc5668fb014f283`, including both parents of that
merge commit, is Legacy Materials. Later copies and adaptations retain the
applicable legacy license for their legacy portions.

At the cutoff, the principal directory licenses were:

| Cutoff path | Legacy license |
| --- | --- |
| `server/` | MIT |
| `sdk-go/` | Apache License 2.0 |
| `sdk-java/` | Apache License 2.0 |
| `sdk-python/` | Apache License 2.0 |
| `protos/` | MIT or Apache License 2.0, at the recipient's option |
| `samples-go/` | MIT |
| `samples-java/` | Apache License 2.0 |
| `samples-python/` | Apache License 2.0 |

The MIT and Apache License 2.0 texts are in `LICENSES/`. Historical copyright
and attribution notices remain authoritative and must be preserved. Git history
identifies the exact material present at the cutoff.

## Excluded directories

This relicensing does not cover `docs/` or `examples/`.

- `examples/go/` remains under its existing MIT License.
- `examples/java/` and `examples/python/` remain under their existing Apache
  License 2.0 terms.
- `docs/` retains its legacy copyright and licensing status. This repository
  makes no new license grant for that directory.

## Existing third-party notices

Some source files retain notices naming Cadence workflow OSS organization,
Uber Technologies, Inc., Indeed, Inc., individual contributors, or other
holders. Those notices and their accompanying licenses remain in force.

Dependencies not copied into this repository remain governed by the license
distributed with each dependency.

## elkjs (Eclipse Layout Kernel)

Dex Web lays out Flow Definition Graphs with elkjs 0.11.1, the Eclipse Layout
Kernel compiled to JavaScript.

- Copyright (c) 2017 Kiel University and others.
- Licensed under the Eclipse Public License 2.0. The license text is in
  `LICENSES/EPL-2.0.txt` and at https://www.eclipse.org/legal/epl-2.0/.
- Source code: https://github.com/kieler/elkjs/tree/0.11.1, also published as
  the npm package https://www.npmjs.com/package/elkjs/v/0.11.1.

elkjs is distributed unmodified, as part of the Dex Web bundle that `dexcli`,
the Dex Server image and the documentation site include. The Sustainable Use
License 1.0 does not apply to it.

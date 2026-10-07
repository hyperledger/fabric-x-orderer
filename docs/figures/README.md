<!--
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
-->
# Figures

The documentation figures share one theme, so that they look alike and follow the reader's light or
dark GitHub theme.

- `theme.json` holds the font, the font sizes, and a light and a dark value for every colour.
- `src/<name>.svg` is a figure's source. It holds layout and class names only, never a colour or a font.
- `<name>.svg` and `<name>-dark.svg` are generated from the two. Do not edit them by hand.

After changing a source or the theme, rebuild every figure:

```bash
python3 scripts/build_figures.py
```

The script needs only the Python standard library. It inlines the resolved style into each output,
because an SVG shown as an image cannot load an external stylesheet. Commit the sources and the
outputs together.

## Using a figure

Reference both variants with a `<picture>` element, and GitHub picks the one matching the reader's
theme:

```html
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="figures/<name>-dark.svg">
  <img alt="<description>" src="figures/<name>.svg">
</picture>
```

## Classes

| Class | Draws |
|-------|-------|
| `node` | A node box; label it with a `node-label` text, `node-label-main` for the figure's subject, or `node-label-small` for a unit inside a node. |
| `node batches` | A node filled in the batch colour, for a batch or what it supplies. |
| `node-sub` | A `tspan` subscript in a node label, such as the shard in `B1s1`. |
| `ledger` | A ledger cylinder. |
| `group` with `shard-1`, `shard-2`, `shard-3` or `consensus` | A rounded group behind nodes. |
| `group frame` | A dashed boundary around the units of one node, with no fill. |
| `group-label` | A group's name. |
| `flow` with `txs`, `digests`, `batches`, `blocks`, `control` or `requests` | A line or path carrying that kind of data; add `dashed` for an occasional path. |
| `flow-label` with the same flow class | A label on that flow, in its colour. |
| `m-<flow>` | The arrowhead path of a marker for that flow. |
| `primary` | The primary marker, such as the star on a shard's primary batcher. |
| `note` | Small, muted text. |
| `small` | Text at the note size, in its own colour; for labels on short flows. |
| `start`, `end` | Left- or right-anchored text; text is centred otherwise. |

Each source contains a `<!-- theme -->` marker, which the script replaces with the style.

## Colours

The palette is GitHub's Primer, so that figures sit naturally on the page in both themes.

| Token | Used for | Light | Dark |
|-------|----------|-------|------|
| `text` | labels | `#1F2328` | `#F0F6FC` |
| `muted` | notes | `#59636E` | `#9198A1` |
| `node-fill`, `node-stroke` | node boxes | `#FFFFFF`, `#1F2328` | `#151B23`, `#D1D7E0` |
| `group-stroke`, `group-label` | group outlines and names | `#D1D9E0`, `#0969DA` | `#3D444D`, `#4493F8` |
| `shard-1`, `shard-2`, `shard-3` | shard fills | `#F6F8FA`, `#DDF4FF`, `#FFF8C5` | `#212830`, `#121D2F`, `#272115` |
| `consensus` | consensus fill | `#FBEFFF` | `#252138` |
| `batches-fill` | a node holding a batch (`node batches`) | `#DAFBE1` | `#12261E` |
| `txs` | transactions | `#BC4C00` | `#DB6D28` |
| `digests` | BAFs, BAs and decisions | `#8250DF` | `#AB7DF8` |
| `batches` | batches | `#1A7F37` | `#3FB950` |
| `blocks` | blocks | `#1F2328` | `#F0F6FC` |
| `control` | the control path | `#CF222E` | `#F85149` |
| `requests` | requests for a batch by its batchID | `#59636E` | `#9198A1` |
| `primary` | the primary marker | `#9A6700` | `#D29922` |

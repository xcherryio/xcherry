import React, {type ReactNode} from 'react';

/**
 * The Dex mark: the pipe operator |> set as a D, passing a result to the next step.
 *
 * The same geometry as the app's mark (web/app/components/DexMark.tsx), kept as a
 * second small component rather than shared through a package because the two
 * sites have no build relationship and one 40-line SVG is cheaper than a
 * dependency. If the geometry changes, both change — the numbers are the contract.
 *
 * Inline rather than an <img>, so the two flats come from the docs theme's own
 * custom properties and the mark follows light and dark for free. The asset it
 * replaces was a 1254x1254 PNG with no alpha that had to be cropped inside a
 * circle with negative offsets.
 */
export default function DexMark({size = 30}: {size?: number}): ReactNode {
  // A 64-unit grid. At 32px and below, heavier strokes keep the shape legible.
  const isSmall = size <= 32;
  const strokeWidth = isSmall ? 12 : 10.5;
  const top = isSmall ? 12 : 11;
  const bottom = 64 - top;
  const chevronStart = isSmall ? 30.5 : 29.5;
  return (
    <svg
      aria-hidden="true"
      focusable="false"
      height={size}
      viewBox="0 0 64 64"
      width={size}
      xmlns="http://www.w3.org/2000/svg">
      <path
        d={`M 13.5 ${top} V ${bottom}`}
        fill="none"
        stroke="var(--brand-track)"
        strokeLinecap="round"
        strokeWidth={strokeWidth}
      />
      <path
        d={`M ${chevronStart} ${top} L 50.5 32 L ${chevronStart} ${bottom}`}
        fill="none"
        stroke="var(--brand-head)"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={strokeWidth}
      />
    </svg>
  );
}

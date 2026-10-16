// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/**
 * The Dex mark: the pipe operator |> set as a D, passing a result to the next step.
 *
 * The dark bar is what has already happened and cannot un-happen; the bright
 * chevron is the hand-off to the next step. Two flat colours, no gradient — a
 * gradient cannot be a token.
 *
 * Inline SVG rather than an <img>, because the mark it replaces was a 1254x1254
 * 8-bit PNG with no alpha: it could not be tinted, could not sit on a dark
 * ground, and needed a wrapper that cropped it with negative offsets. These two
 * strokes are CSS custom properties, so the mark follows the theme for free and
 * the chevron is the same token as the primary button.
 *
 * The chevron arms run at exactly 45 degrees, so it reads as a typed > and not a
 * play triangle.
 */
export function DexMark({ size = 28 }: { size?: number }) {
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
      xmlns="http://www.w3.org/2000/svg"
    >
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

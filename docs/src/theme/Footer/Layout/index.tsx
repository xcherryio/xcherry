/*
 * Copyright (c) 2026 Super Durable, Inc.
 *
 * Licensed under the Super Durable Source License 1.0.
 * You may not use this file except in compliance with the License.
 * See the LICENSE file in the repository root.
 *
 * SPDX-License-Identifier: LicenseRef-Super-Durable-1.0
 */

import React, {type ReactNode} from 'react';
import clsx from 'clsx';
import NavbarLogo from '@theme/Navbar/Logo';
import type {Props} from '@theme/Footer/Layout';

export default function FooterLayout({style, links, copyright}: Props): ReactNode {
  return (
    <footer className={clsx('theme-layout-footer footer', {'footer--dark': style === 'dark'})}>
      <div className="container container-fluid">
        <div className="company-footer-top">
          <div className="company-footer-brand"><NavbarLogo /></div>
          {links}
        </div>
        {copyright && <div className="company-footer-bottom">{copyright}</div>}
      </div>
    </footer>
  );
}

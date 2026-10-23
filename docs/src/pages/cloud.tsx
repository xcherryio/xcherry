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
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';

export default function CloudPage(): ReactNode {
  return (
    <Layout
      title="Dex Cloud / BYOC"
      description="Managed and bring-your-own-cloud deployment options for Dex.">
      <main className="cloud-page">
        <p className="eyebrow">DEX CLOUD / BYOC</p>
        <h1>Dex Cloud / BYOC</h1>
        <p className="cloud-status">Coming Soon</p>
        <p className="cloud-description">
          Managed Dex and private deployments inside your own cloud boundary are in development.
          We are designing the same durable execution model for teams that need dedicated control,
          security, and operational support.
        </p>
        <div className="cloud-actions">
          <Link className="brand-button brand-button-primary" to="/">
            Explore Dex OSS Docs <span aria-hidden="true">→</span>
          </Link>
          <a className="brand-button" href="https://superdurable.ai/dex-engine/">
            View Dex BYOC <span aria-hidden="true">↗</span>
          </a>
        </div>
      </main>
    </Layout>
  );
}

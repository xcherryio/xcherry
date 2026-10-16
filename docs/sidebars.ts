import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    'index',
    {
      type: 'category',
      label: 'Intro and Quick Start',
      items: ['intro/what-is-durable-execution', 'intro/what-is-dex', 'quick-start/index'],
    },
    {
      type: 'category',
      label: 'Primitives',
      link: {type: 'doc', id: 'primitives/index'},
      items: [
        {
          type: 'category',
          label: 'Flow',
          link: {type: 'doc', id: 'primitives/flow/index'},
          items: ['primitives/flow/basic', 'primitives/flow/flow-options'],
        },
        {
          type: 'category',
          label: 'Step',
          link: {type: 'doc', id: 'primitives/step/index'},
          items: [
            'primitives/step/basic',
            'primitives/step/waiting-condition',
            'primitives/step/step-decision',
            'primitives/step/step-options',
          ],
        },
        'primitives/connector',
        'primitives/attribute',
        'primitives/rpc',
        'primitives/channel',
        'primitives/stream',
        'primitives/timer',
        'primitives/subflow',
        {
          type: 'category',
          label: 'Client',
          link: {type: 'doc', id: 'primitives/client/index'},
          items: ['primitives/client/basic', 'primitives/client/advanced'],
        },
      ],
    },
    {
      type: 'category',
      label: 'Build with AI',
      collapsed: false,
      items: [
        'build-with-ai/dex-developer-skill',
        'build-with-ai/dex-developer-skill/dex-app-builder',
        'build-with-ai/dex-developer-skill/dex-sdk',
        'build-with-ai/dex-developer-skill/dex-connector-contributor',
      ],
    },
    {
      type: 'category',
      label: 'Design Patterns',
      link: {type: 'doc', id: 'design-patterns/index'},
      items: [
        {
          type: 'category',
          label: 'Failure Handling',
          link: {type: 'doc', id: 'design-patterns/failure-handling'},
          items: [
            'design-patterns/failure-handling/execute-failure-recovery',
            'design-patterns/failure-handling/manual-recovery',
            'design-patterns/failure-handling/graceful-timeout',
            'design-patterns/failure-handling/wait-for-failure-recovery',
          ],
        },
        'design-patterns/polling',
        {
          type: 'category',
          label: 'Durable Timer',
          link: {type: 'doc', id: 'design-patterns/durable-timer/index'},
          items: [
            'design-patterns/durable-timer/cron',
            'design-patterns/durable-timer/reminder',
            'design-patterns/durable-timer/inactiveness-tracker-timer',
          ],
        },
        'design-patterns/interruptible',
        {
          type: 'category',
          label: 'Parallel Steps',
          link: {type: 'doc', id: 'design-patterns/parallel'},
          items: [
            'design-patterns/parallel/static',
            'design-patterns/parallel/dynamic',
            'design-patterns/parallel/await',
            'design-patterns/parallel/first-win',
          ],
        },
        {
          type: 'category',
          label: 'Parallel SubFlows',
          link: {type: 'doc', id: 'design-patterns/parallel-subflows'},
          items: [
            'design-patterns/parallel-subflows/basic',
            'design-patterns/parallel-subflows/wait-for-half',
            'design-patterns/parallel-subflows/long-lived-parent',
            'design-patterns/parallel-subflows/short-lived-parent',
            'design-patterns/parallel-subflows/partitioning',
            'design-patterns/parallel-subflows/back-pressure',
          ],
        },
        {
          type: 'category',
          label: 'Responsive Update',
          link: {type: 'doc', id: 'design-patterns/responsive-update'},
          items: [
            'design-patterns/responsive-update/step-completion',
            'design-patterns/responsive-update/attribute-match',
            'design-patterns/responsive-update/stream',
          ],
        },
        {
          type: 'category',
          label: 'Drain Channels',
          items: [
            'design-patterns/drain-internal-channels',
            'design-patterns/draining-channel-for-external-publishing',
          ],
        },
        'design-patterns/sequentially-chunked-attribute-map',
        'design-patterns/hash-partitioned-attribute-map',
        'design-patterns/entity-store',
      ],
    },
    {
      type: 'category',
      label: 'Product Examples',
      link: {type: 'doc', id: 'use-cases/index'},
      items: [
        'use-cases/ai-agent',
        'use-cases/subscription',
        'use-cases/user-onboarding-process',
        'use-cases/money-transfer',
        'use-cases/job-seeker-engagement',
        'use-cases/deal-dsl',
        'use-cases/microservice-orchestration',
        'use-cases/job-post',
        'use-cases/customer-refund',
      ],
    },
    {
      type: 'category',
      label: 'Deploy to Production',
      link: {type: 'doc', id: 'production/index'},
      items: [
        'production/application-operations',
        'production/pull-workers',
        'production/dex-web',
        'production/local-connector-setup',
        'production/server-operations',
        'production/metrics',
        'production/attribute-store',
      ],
    },
    {
      type: 'category',
      label: 'References',
      link: {type: 'doc', id: 'references/index'},
      items: ['references/cli', 'references/flow-visualization'],
    },
    'glossary',
  ],
};

export default sidebars;

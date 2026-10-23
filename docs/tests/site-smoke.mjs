import assert from 'node:assert/strict';
import {access, readFile, readdir} from 'node:fs/promises';
import {dirname, join, relative, sep} from 'node:path';
import {fileURLToPath} from 'node:url';

const siteOrigin = 'https://docs.superdurable.io';
const root = join(dirname(fileURLToPath(import.meta.url)), '..', 'build');
const docsRoot = join(root, '..');
const redirectsPath = join(root, '..', 'redirects.json');
const redirectRules = JSON.parse(await readFile(redirectsPath, 'utf8'));
const expectedRedirects = new Map(
  redirectRules.map(({from, to}) => [
    routeWithTrailingSlash(from),
    new URL(routeWithTrailingSlash(to), siteOrigin).href,
  ]),
);

const home = await readFile(join(root, 'index.html'), 'utf8');
const cloud = await readFile(join(root, 'cloud', 'index.html'), 'utf8');
const cron = await readFile(join(root, 'design-patterns', 'durable-timer', 'cron', 'index.html'), 'utf8');
const whyDex = await readFile(join(root, 'intro', 'what-is-dex', 'index.html'), 'utf8');
const production = await readFile(join(root, 'production', 'index.html'), 'utf8');
const dexSkills = await readFile(join(root, 'build-with-ai', 'dex-developer-skill', 'index.html'), 'utf8');
const dexAppBuilder = await readFile(
  join(root, 'build-with-ai', 'dex-developer-skill', 'dex-app-builder', 'index.html'),
  'utf8',
);
const dexSdk = await readFile(join(root, 'build-with-ai', 'dex-developer-skill', 'dex-sdk', 'index.html'), 'utf8');
const dexConnectorContributor = await readFile(
  join(root, 'build-with-ai', 'dex-developer-skill', 'dex-connector-contributor', 'index.html'),
  'utf8',
);
const connector = await readFile(join(root, 'primitives', 'connector', 'index.html'), 'utf8');
const zhCron = await readFile(join(root, 'zh-Hans', 'design-patterns', 'durable-timer', 'cron', 'index.html'), 'utf8');
const zhDexSkills = await readFile(join(root, 'zh-Hans', 'build-with-ai', 'dex-developer-skill', 'index.html'), 'utf8');
const zhDexAppBuilder = await readFile(
  join(root, 'zh-Hans', 'build-with-ai', 'dex-developer-skill', 'dex-app-builder', 'index.html'),
  'utf8',
);
const zhDexSdk = await readFile(
  join(root, 'zh-Hans', 'build-with-ai', 'dex-developer-skill', 'dex-sdk', 'index.html'),
  'utf8',
);
const zhDexConnectorContributor = await readFile(
  join(root, 'zh-Hans', 'build-with-ai', 'dex-developer-skill', 'dex-connector-contributor', 'index.html'),
  'utf8',
);
const zhConnector = await readFile(join(root, 'zh-Hans', 'primitives', 'connector', 'index.html'), 'utf8');
const zhWhyDex = await readFile(join(root, 'zh-Hans', 'intro', 'what-is-dex', 'index.html'), 'utf8');
const zhSubflow = await readFile(join(root, 'zh-Hans', 'primitives', 'subflow', 'index.html'), 'utf8');
const dexSkillHostTabsSource = await readFile(join(docsRoot, 'src', 'components', 'DexSkillHostTabs.tsx'), 'utf8');
const dexSkillSourcePaths = [
  join(docsRoot, 'content', 'build-with-ai', 'dex-developer-skill.mdx'),
  join(docsRoot, 'content', 'build-with-ai', 'dex-developer-skill', 'dex-app-builder.mdx'),
  join(docsRoot, 'content', 'build-with-ai', 'dex-developer-skill', 'dex-sdk.mdx'),
  join(docsRoot, 'content', 'build-with-ai', 'dex-developer-skill', 'dex-connector-contributor.mdx'),
  join(
    docsRoot,
    'i18n',
    'zh-Hans',
    'docusaurus-plugin-content-docs',
    'current',
    'build-with-ai',
    'dex-developer-skill.mdx',
  ),
  join(
    docsRoot,
    'i18n',
    'zh-Hans',
    'docusaurus-plugin-content-docs',
    'current',
    'build-with-ai',
    'dex-developer-skill',
    'dex-app-builder.mdx',
  ),
  join(
    docsRoot,
    'i18n',
    'zh-Hans',
    'docusaurus-plugin-content-docs',
    'current',
    'build-with-ai',
    'dex-developer-skill',
    'dex-sdk.mdx',
  ),
  join(
    docsRoot,
    'i18n',
    'zh-Hans',
    'docusaurus-plugin-content-docs',
    'current',
    'build-with-ai',
    'dex-developer-skill',
    'dex-connector-contributor.mdx',
  ),
];
const dexSkillSources = await Promise.all(dexSkillSourcePaths.map((filePath) => readFile(filePath, 'utf8')));

assert.match(home, /https:\/\/superdurable\.ai\/dex-ai\//);
assert.match(home, /https:\/\/github\.com\/superdurable/);
assert.match(home, /Toggle color theme/);
assert.match(home, />Sign up\s*</);
assert.doesNotMatch(home, /github-star-link|github-star-stat|Book a call|Book a demo/);

const navbar = home.match(/<nav aria-label="Main"[\s\S]*?<\/nav>/)?.[0] ?? '';
assert.match(navbar, />Platform<[\s\S]*?>Engine<[\s\S]*?>About</);
assert.match(navbar, /navbar__link--active[^>]*>Engine</);
assert.doesNotMatch(navbar, />Docs</);
assert.match(navbar, /href="https:\/\/dex\.superdurable\.ai\/login"/);
assert.match(navbar, /href="https:\/\/dex\.superdurable\.ai\/register"/);
assert.doesNotMatch(navbar, /target="_blank"/);
assert.doesNotMatch(home, /product-bar|https:\/\/superdurable\.io\//);

const footer = home.match(/<footer class="theme-layout-footer footer[\s\S]*?<\/footer>/)?.[0] ?? '';
assert.match(footer, />Products</);
assert.match(footer, />Developers</);
assert.match(footer, />Company</);
assert.match(footer, />Dex AI Platform</);
assert.match(footer, />Managed Dex Engine</);
assert.match(footer, /href="\/quick-start\/"/);
assert.match(footer, /href="https:\/\/superdurable\.ai\/about\/#contact"/);
assert.match(footer, /Super Durable, Inc\./);
assert.doesNotMatch(footer, /Process-centric apps|Simple, reliable|>Team<|Book a/);
assert.match(home, /English/);
assert.match(home, /中文/);
assert.match(home, /Build with AI/);
assert.match(home, /href="\/build-with-ai\/dex-developer-skill\/">Overview<\/a>/);

const primitivesPosition = home.indexOf('>Primitives<');
const buildWithAIPosition = home.indexOf('>Build with AI<');
const designPatternsPosition = home.indexOf('>Design Patterns<');
assert.ok(primitivesPosition >= 0);
assert.ok(buildWithAIPosition > primitivesPosition);
assert.ok(designPatternsPosition > buildWithAIPosition);

const zhHome = await readFile(join(root, 'zh-Hans', 'index.html'), 'utf8');
assert.match(zhHome, /English/);
assert.match(zhHome, /中文/);
for (const label of ['平台', '引擎', '文档', '关于', '登录', '注册', '产品', '开发者', '公司', '快速开始', '引擎对比', '联系我们']) {
  assert.ok(zhHome.includes(label), `Missing Chinese shell label: ${label}`);
}
assert.match(zhHome, /href="\/zh-Hans\/quick-start\/"/);

assert.match(cloud, /Dex Cloud \/ BYOC/);
assert.match(cloud, /Coming Soon/);
assert.match(cloud, /Explore Dex OSS Docs/);
assert.match(cloud, /https:\/\/superdurable\.ai\/dex-engine\//);
assert.match(cron, /flow-definition-canvas/);
assert.match(cron, /CronScheduleFlow/);
assert.match(zhCron, /flow-definition-canvas/);
assert.match(zhCron, /CronScheduleFlow/);
assert.match(whyDex, /flow-definition-canvas/);
assert.match(whyDex, /OrderProcessingFlow/);
assert.match(zhWhyDex, /flow-definition-canvas/);
assert.match(zhWhyDex, /OrderProcessingFlow/);
assert.match(production, /rel="canonical" href="https:\/\/docs\.superdurable\.io\/production\/"/);
assert.match(dexSkills, /Build with AI/);
assert.match(dexSkills, /superdurable\/dex-skills/);
assert.match(dexSkills, /dex-sdk/);
assert.match(dexSkills, /dex-app-builder/);
assert.match(dexSkills, /dex-connector-contributor/);
assert.match(dexSkills, /primary skill/i);
assert.match(dexSkills, /Requires a coding workspace/);
assert.match(dexSkills, /writable repository or project workspace/);
assert.match(dexSkills, /https:\/\/github\.com\/superdurable\/dex-skills/);

const englishDexSkillPages = [dexSkills, dexAppBuilder, dexSdk, dexConnectorContributor];
const chineseDexSkillPages = [zhDexSkills, zhDexAppBuilder, zhDexSdk, zhDexConnectorContributor];
for (const page of englishDexSkillPages) {
  assertDexSkillHostTabs(page, 'Other clients', false);
}
for (const page of chineseDexSkillPages) {
  assertDexSkillHostTabs(page, '其他客户端', true);
}

assert.match(dexSkillHostTabsSource, /defaultValue="codex-desktop"/);
assert.match(dexSkillHostTabsSource, /groupId="dex-skill-host"/);
assert.match(dexSkillHostTabsSource, /queryString="host"/);
assert.equal((dexSkillHostTabsSource.match(/<TabItem\b/g) ?? []).length, 6);
assert.doesNotMatch(dexSkillHostTabsSource, /\blazy(?:=|\s)/);
for (const source of dexSkillSources) {
  assert.match(source, /<DexSkillHostTabs skill="(?:all|dex-app-builder|dex-sdk|dex-connector-contributor)" \/>/);
  assert.doesNotMatch(source, /^## (?:Install for|安装到) (?:Codex|Claude|Cursor)$/m);
  assert.doesNotMatch(source, /\[(?:installation overview|安装总览)\]/i);
}

const dexSkillsText = htmlText(dexSkills);
assert.match(dexSkillsText, /\$dex-app-builder Design and build an event-registration application\./);
assert.match(dexSkillsText, /\$dex-sdk Diagnose why this Flow retries after the payment Step completes\./);
assert.match(dexSkillsText, /\$dex-connector-contributor Add <XYZ> to Dex official connector library/);
assert.match(dexSkillsText, /\/superdurable-dex:dex-connector-contributor Add <XYZ>/);
assert.doesNotMatch(dexSkillsText, /Add this Trigger to the official Slack Connector/);

assert.match(htmlText(dexAppBuilder), /\$dex-app-builder Design and build an event-registration application\./);
assert.match(htmlText(dexSdk), /\$dex-sdk Diagnose why this Flow retries after the payment Step completes\./);
assert.match(
  htmlText(dexConnectorContributor),
  /\$dex-connector-contributor Add <XYZ> to Dex official connector library/,
);
for (const page of [dexAppBuilder, zhDexAppBuilder]) {
  assertSkillInvocationExamples(page, 'dex-app-builder', 'Design and build an event-registration application.');
}
for (const page of [dexSdk, zhDexSdk]) {
  assertSkillInvocationExamples(page, 'dex-sdk', 'Diagnose why this Flow retries after the payment Step completes.');
}
for (const page of [dexConnectorContributor, zhDexConnectorContributor]) {
  assertSkillInvocationExamples(page, 'dex-connector-contributor', 'Add <XYZ> to Dex official connector library');
}
assert.ok(dexSkills.indexOf('dex-app-builder') < dexSkills.indexOf('dex-sdk'));
assert.match(dexSkills, /dex-developer-skill\/dex-app-builder/);
assert.match(dexSkills, /dex-developer-skill\/dex-sdk/);
assert.match(dexSkills, /dex-developer-skill\/dex-connector-contributor/);
assert.match(dexAppBuilder, /primary skill/i);
assert.match(dexAppBuilder, /Choose the application surface/);
assert.match(dexAppBuilder, /keep external provider effects in <strong>Execute<\/strong>/);
assert.match(dexSdk, /Version and source authority/);
assert.match(dexSdk, /Python, Go, Java, TypeScript, and Rust/);
assert.match(dexSdk, /Diagnosis is read-only by default/);
assert.match(dexConnectorContributor, /Repository and fork workflow/);
assert.match(dexConnectorContributor, /Update <strong>connector.yaml<\/strong> before provider implementation/);
assert.match(dexConnectorContributor, /production handoff waits for an exact released Connector version/);
assert.match(dexConnectorContributor, /currently support only\s+the Dex Go SDK/);
assert.match(dexConnectorContributor, /https:\/\/github\.com\/superdurable\/dex-connectors-library\/issues\/new/);
assert.match(zhDexSkills, /使用 AI 构建/);
assert.match(zhDexSkills, /主要 skill/);
assert.match(zhDexSkills, /需要代码工作区/);
assert.match(zhDexSkills, /可写 repository 或 project\s+workspace/);
assert.ok(zhDexSkills.indexOf('dex-app-builder') < zhDexSkills.indexOf('dex-sdk'));
assert.match(zhDexAppBuilder, /主要 skill/);
assert.match(zhDexAppBuilder, /选择应用界面/);
assert.match(zhDexSdk, /Version 与 Source Authority/);
assert.match(zhDexSdk, /诊断默认是只读的/);
assert.match(zhDexConnectorContributor, /Repository 与 Fork 工作流/);
assert.match(zhDexConnectorContributor, /production\s+handoff 必须等待精确发布的 Connector version/);
assert.match(zhDexConnectorContributor, /目前只支持 Dex Go SDK/);
assert.match(zhDexConnectorContributor, /https:\/\/github\.com\/superdurable\/dex-connectors-library\/issues\/new/);
assert.match(connector, /independent Dex primitive/);
assert.match(connector, /currently support only\s+the Dex Go SDK/);
assert.match(connector, /https:\/\/github\.com\/superdurable\/dex-connectors-library\/issues\/new/);
assert.match(connector, /rel="canonical" href="https:\/\/docs\.superdurable\.io\/primitives\/connector\/"/);
assert.match(zhConnector, /独立 Dex primitive/);
assert.match(zhConnector, /目前只有使用 Dex Go SDK 的应用可以集成 Connector/);
assert.match(zhConnector, /https:\/\/github\.com\/superdurable\/dex-connectors-library\/issues\/new/);
assert.match(zhConnector, /rel="canonical" href="https:\/\/docs\.superdurable\.io\/zh-Hans\/primitives\/connector\/"/);
assert.match(zhSubflow, /rel="canonical" href="https:\/\/docs\.superdurable\.io\/zh-Hans\/primitives\/subflow\/"/);

await Promise.all([
  access(join(root, 'intro', 'what-is-durable-execution', 'index.html')),
  access(join(root, 'intro', 'what-is-dex', 'index.html')),
  access(join(root, 'quick-start', 'index.html')),
  access(join(root, 'primitives', 'index.html')),
  access(join(root, 'primitives', 'connector', 'index.html')),
  access(join(root, 'primitives', 'step', 'index.html')),
  access(join(root, 'references', 'cli', 'index.html')),
  access(join(root, 'build-with-ai', 'dex-developer-skill', 'index.html')),
  access(join(root, 'build-with-ai', 'dex-developer-skill', 'dex-app-builder', 'index.html')),
  access(join(root, 'build-with-ai', 'dex-developer-skill', 'dex-sdk', 'index.html')),
  access(join(root, 'build-with-ai', 'dex-developer-skill', 'dex-connector-contributor', 'index.html')),
  access(join(root, 'zh-Hans', 'intro', 'what-is-durable-execution', 'index.html')),
  access(join(root, 'zh-Hans', 'intro', 'what-is-dex', 'index.html')),
  access(join(root, 'zh-Hans', 'quick-start', 'index.html')),
  access(join(root, 'zh-Hans', 'primitives', 'connector', 'index.html')),
  access(join(root, 'zh-Hans', 'build-with-ai', 'dex-developer-skill', 'index.html')),
  access(join(root, 'zh-Hans', 'build-with-ai', 'dex-developer-skill', 'dex-app-builder', 'index.html')),
  access(join(root, 'zh-Hans', 'build-with-ai', 'dex-developer-skill', 'dex-sdk', 'index.html')),
  access(join(root, 'zh-Hans', 'build-with-ai', 'dex-developer-skill', 'dex-connector-contributor', 'index.html')),
]);

const outputFiles = await collectOutputFiles(root);
const outputFilePaths = new Set(outputFiles.map((filePath) => relativeOutputPath(filePath)));
const htmlIndexFiles = outputFiles.filter((filePath) => filePath.endsWith(`${sep}index.html`));
const indexablePages = new Map();
const actualRedirects = new Map();

for (const filePath of htmlIndexFiles) {
  const route = routeFromIndexFile(filePath);
  const html = await readFile(filePath, 'utf8');
  const refreshTarget = metaRefreshTarget(html);
  const canonicalLinks = linkTargets(html, 'canonical');

  assert.equal(canonicalLinks.length, 1, `${route} must have exactly one canonical URL`);

  if (refreshTarget !== undefined) {
    const expectedTarget = expectedRedirects.get(route);
    assert.ok(expectedTarget, `${route} is an undeclared redirect`);
    assert.equal(new URL(refreshTarget, siteOrigin).href, expectedTarget);
    assert.equal(new URL(canonicalLinks[0], siteOrigin).href, expectedTarget);
    actualRedirects.set(route, expectedTarget);
    continue;
  }

  const expectedCanonical = new URL(route, siteOrigin).href;
  assert.equal(canonicalLinks[0], expectedCanonical, `${route} has a non-self-referencing canonical URL`);
  assert.deepEqual(
    alternateLanguageLinks(html),
    expectedAlternateLanguageLinks(route),
    `${route} has incorrect alternate-language URLs`,
  );
  assert.equal(hasNoIndexOrNoFollow(html), false, `${route} blocks search indexing`);
  assert.equal(indexablePages.has(expectedCanonical), false, `${route} duplicates canonical ${expectedCanonical}`);
  indexablePages.set(expectedCanonical, {html, route});
}

assert.deepEqual(
  [...actualRedirects.entries()].sort(),
  [...expectedRedirects.entries()].sort(),
  'the production build must contain every declared redirect and no undeclared redirects',
);

for (const [canonical, {html, route}] of indexablePages) {
  for (const href of anchorTargets(html)) {
    const target = new URL(href, canonical);
    if (target.origin !== siteOrigin) {
      continue;
    }
    assert.equal(expectedRedirects.has(target.pathname), false, `${route} links to redirect ${target.pathname}`);
    assert.ok(
      indexablePages.has(new URL(target.pathname, siteOrigin).href) ||
        outputFilePaths.has(decodeURIComponent(target.pathname).replace(/^\//, '')),
      `${route} links to missing route ${target.pathname}`,
    );
  }
}

const sitemapFiles = outputFiles.filter((filePath) => filePath.endsWith(`${sep}sitemap.xml`));
const sitemapUrls = (
  await Promise.all(
    sitemapFiles.map(async (filePath) => {
      const sitemap = await readFile(filePath, 'utf8');
      return [...sitemap.matchAll(/<loc>([^<]+)<\/loc>/g)].map((match) => match[1]);
    }),
  )
).flat();
assert.equal(new Set(sitemapUrls).size, sitemapUrls.length, 'sitemap URLs must be unique');
assert.deepEqual(
  [...sitemapUrls].sort(),
  [...indexablePages.keys()].sort(),
  'sitemap must contain every indexable canonical URL and no redirects or duplicate variants',
);

const robots = await readFile(join(root, 'robots.txt'), 'utf8');
assert.match(robots, /^User-agent: \*$/m);
assert.match(robots, /^Allow: \/$/m);
assert.doesNotMatch(robots, /^Disallow:\s*\/$/m);
assert.match(robots, /^Sitemap: https:\/\/docs\.superdurable\.io\/sitemap\.xml$/m);
assert.match(robots, /^Sitemap: https:\/\/docs\.superdurable\.io\/zh-Hans\/sitemap\.xml$/m);

console.log(
  `Docs shell and SEO audit passed for ${indexablePages.size} indexable routes and ${actualRedirects.size} redirects.`,
);

function assertDexSkillHostTabs(html, otherClientsLabel, isChinese) {
  const tabLabels = [...html.matchAll(/role="tab"[^>]*>([^<]+)<\/li>/g)].map((match) => match[1]);
  assert.deepEqual(tabLabels, [
    'Codex Desktop',
    'Codex CLI',
    'Claude Desktop',
    'Claude Code CLI',
    'Cursor',
    otherClientsLabel,
  ]);
  assert.match(html, /role="tab"[^>]*aria-selected="true"[^>]*>Codex Desktop<\/li>/);
  assert.equal((html.match(/role="tabpanel"/g) ?? []).length, 6);

  const text = htmlText(html);
  assert.match(text, /@Dex /);
  assert.match(text, /\/superdurable-dex:(?:dex-app-builder|dex-sdk|dex-connector-contributor) /);
  assert.match(text, /codex plugin marketplace add superdurable\/dex-skills/);
  assert.match(text, /codex plugin add superdurable-dex@superdurable/);
  assert.match(text, /npx skills add superdurable\/dex-skills --skill '\*' --agent codex --global/);
  assert.match(text, /claude plugin marketplace add superdurable\/dex-skills/);
  assert.match(text, /claude plugin install superdurable-dex@superdurable/);
  assert.match(text, /npx skills add superdurable\/dex-skills --skill '\*' --agent claude-code --global/);
  assert.match(text, /cursor-agent/);
  assert.match(text, /npx skills add superdurable\/dex-skills --skill '\*' --agent cursor --global/);
  assert.match(text, /npx skills add superdurable\/dex-skills --skill '\*' --global/);
  assert.match(text, /npx skills add superdurable\/dex-skills --skill '\*' --agent <agent-id> --global/);
  assert.match(text, /npx skills update dex-app-builder dex-sdk dex-connector-contributor --global/);
  assert.match(text, isChinese ? /Plugin（推荐）/ : /Plugin \(recommended\)/);
  assert.match(text, /Standalone Skills/);
  assert.match(text, isChinese ? /安装.*使用.*更新/s : /Install.*Use.*Update/s);
}

function assertSkillInvocationExamples(html, skillName, prompt) {
  const text = htmlText(html);
  assert.ok(text.includes(`@Dex ${prompt}`));
  assert.ok(text.includes(`$${skillName} ${prompt}`));
  assert.ok(text.includes(`/superdurable-dex:${skillName} ${prompt}`));
  assert.ok(text.includes(`/${skillName} ${prompt}`));
  for (const otherPrompt of [
    'Design and build an event-registration application.',
    'Diagnose why this Flow retries after the payment Step completes.',
    'Add <XYZ> to Dex official connector library',
  ]) {
    if (otherPrompt !== prompt) {
      assert.equal(text.includes(otherPrompt), false);
    }
  }
}

function htmlText(html) {
  return html
    .replace(/<[^>]*>/g, '')
    .replaceAll('&lt;', '<')
    .replaceAll('&gt;', '>')
    .replaceAll('&#x27;', "'")
    .replaceAll('&quot;', '"')
    .replaceAll('&amp;', '&');
}

async function collectOutputFiles(directory) {
  const entries = await readdir(directory, {withFileTypes: true});
  const files = await Promise.all(
    entries.map((entry) => {
      const entryPath = join(directory, entry.name);
      return entry.isDirectory() ? collectOutputFiles(entryPath) : [entryPath];
    }),
  );
  return files.flat();
}

function relativeOutputPath(filePath) {
  return relative(root, filePath).split(sep).join('/');
}

function routeFromIndexFile(filePath) {
  const outputPath = relativeOutputPath(filePath);
  if (outputPath === 'index.html') {
    return '/';
  }
  return `/${outputPath.slice(0, -'index.html'.length)}`;
}

function routeWithTrailingSlash(pathname) {
  const route = new URL(pathname, siteOrigin).pathname.replace(/\/+$/, '');
  return route === '' ? '/' : `${route}/`;
}

function htmlTags(html, tagName) {
  return html.match(new RegExp(`<${tagName}\\b[^>]*>`, 'gi')) ?? [];
}

function attributeValue(tag, attributeName) {
  const match = tag.match(new RegExp(`\\b${attributeName}=(['"])(.*?)\\1`, 'i'));
  return match?.[2];
}

function linkTargets(html, relationship) {
  return htmlTags(html, 'link')
    .filter((tag) => attributeValue(tag, 'rel')?.split(/\s+/).includes(relationship))
    .map((tag) => attributeValue(tag, 'href'))
    .filter((href) => href !== undefined);
}

function anchorTargets(html) {
  return htmlTags(html, 'a')
    .map((tag) => attributeValue(tag, 'href'))
    .filter((href) => href !== undefined && !/^(?:mailto:|tel:|javascript:)/i.test(href));
}

function alternateLanguageLinks(html) {
  const entries = htmlTags(html, 'link')
    .filter((tag) => attributeValue(tag, 'rel')?.split(/\s+/).includes('alternate'))
    .map((tag) => [attributeValue(tag, 'hreflang'), attributeValue(tag, 'href')]);
  assert.equal(
    new Set(entries.map(([language]) => language)).size,
    entries.length,
    'alternate-language declarations must be unique',
  );
  return Object.fromEntries(entries);
}

function expectedAlternateLanguageLinks(route) {
  const unlocalizedRoute = route.startsWith('/zh-Hans/')
    ? route.slice('/zh-Hans'.length)
    : route;
  const englishUrl = new URL(unlocalizedRoute, siteOrigin).href;
  const chineseUrl = new URL(`/zh-Hans${unlocalizedRoute}`, siteOrigin).href;
  return {
    en: englishUrl,
    'zh-Hans': chineseUrl,
    'x-default': englishUrl,
  };
}

function metaRefreshTarget(html) {
  const refreshTag = htmlTags(html, 'meta').find(
    (tag) => attributeValue(tag, 'http-equiv')?.toLowerCase() === 'refresh',
  );
  if (refreshTag === undefined) {
    return undefined;
  }
  const content = attributeValue(refreshTag, 'content') ?? '';
  return content.match(/^\s*0\s*;\s*url=(.+)\s*$/i)?.[1];
}

function hasNoIndexOrNoFollow(html) {
  return htmlTags(html, 'meta').some((tag) => {
    if (attributeValue(tag, 'name')?.toLowerCase() !== 'robots') {
      return false;
    }
    return /(?:^|[,\s])(noindex|nofollow)(?:$|[,\s])/i.test(attributeValue(tag, 'content') ?? '');
  });
}

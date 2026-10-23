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
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {useAlternatePageUtils} from '@docusaurus/theme-common/internal';
import {useLocation} from '@docusaurus/router';
import {translate} from '@docusaurus/Translate';
import DropdownNavbarItem from '@theme/NavbarItem/DropdownNavbarItem';
import type {Props} from '@theme/NavbarItem/LocaleDropdownNavbarItem';
import {isDocsLocale, persistDocsLocale} from '@site/src/components/docsLocale';

export default function LocaleDropdownNavbarItem({
  mobile,
  dropdownItemsBefore = [],
  dropdownItemsAfter = [],
  queryString = '',
  ...props
}: Props): ReactNode {
  const {i18n: {currentLocale, locales, localeConfigs}} = useDocusaurusContext();
  const alternatePageUtils = useAlternatePageUtils();
  const {search, hash} = useLocation();
  const localeItems = locales.map((locale) => ({
    label: localeConfigs[locale]!.label,
    lang: localeConfigs[locale]!.htmlLang,
    to: `pathname://${alternatePageUtils.createUrl({locale, fullyQualified: false})}${search}${hash}${queryString}`,
    target: '_self',
    autoAddBaseUrl: false,
    className: locale === currentLocale ? (mobile ? 'menu__link--active' : 'dropdown__link--active') : '',
    onClick: () => {
      if (isDocsLocale(locale)) {
        persistDocsLocale(locale);
      }
    },
  }));

  return (
    <DropdownNavbarItem
      {...props}
      mobile={mobile}
      label={mobile ? translate({id: 'theme.navbar.mobileLanguageDropdown.label', message: 'Languages'}) : localeConfigs[currentLocale]!.label}
      items={[...dropdownItemsBefore, ...localeItems, ...dropdownItemsAfter]}
    />
  );
}

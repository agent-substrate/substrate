// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Adapted from Docsy's offline-search.js (Matt Walters), extended to extract
// contextual match snippets from doc.body and highlight query terms.

(function ($) {
  'use strict';

  function escapeRegExp(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function queryTokens(rawQuery) {
    return Array.from(
      new Set(
        lunr
          .tokenizer(rawQuery.toLowerCase())
          .map((t) => t.toString().trim())
          .filter((t) => t.length > 0)
      )
    );
  }

  function buildSnippet(doc, rawQuery, tokens) {
    const body = (doc.body || doc.excerpt || '').replace(/\s+/g, ' ').trim();
    if (!body) {
      return '';
    }

    const lowerBody = body.toLowerCase();
    const trimmedQuery = rawQuery.trim().toLowerCase();

    let matchIndex = -1;
    let matchLen = 0;

    if (trimmedQuery.length > 1) {
      matchIndex = lowerBody.indexOf(trimmedQuery);
      if (matchIndex !== -1) {
        matchLen = trimmedQuery.length;
      }
    }

    if (matchIndex === -1) {
      for (const token of tokens) {
        const idx = lowerBody.indexOf(token);
        if (idx !== -1 && (matchIndex === -1 || idx < matchIndex)) {
          matchIndex = idx;
          matchLen = token.length;
        }
      }
    }

    if (matchIndex === -1) {
      const fallback = (doc.excerpt || body).replace(/\s+/g, ' ').trim();
      return fallback.length > 200 ? fallback.slice(0, 200).trimEnd() + '…' : fallback;
    }

    const radiusBefore = 70;
    const radiusAfter = 130;
    let start = Math.max(0, matchIndex - radiusBefore);
    let end = Math.min(body.length, matchIndex + matchLen + radiusAfter);

    if (start > 0) {
      const spaceIdx = body.indexOf(' ', start);
      if (spaceIdx !== -1 && spaceIdx < matchIndex) {
        start = spaceIdx + 1;
      }
    }
    if (end < body.length) {
      const spaceIdx = body.lastIndexOf(' ', end);
      if (spaceIdx !== -1 && spaceIdx > matchIndex + matchLen) {
        end = spaceIdx;
      }
    }

    const prefix = start > 0 ? '…' : '';
    const suffix = end < body.length ? '…' : '';
    return prefix + body.slice(start, end).trim() + suffix;
  }

  function appendHighlighted($container, text, tokens) {
    if (!text) {
      return;
    }
    const validTokens = tokens
      .filter((t) => t.length >= 2)
      .sort((a, b) => b.length - a.length);
    if (validTokens.length === 0) {
      $container.text(text);
      return;
    }

    const pattern = new RegExp(
      '(' + validTokens.map(escapeRegExp).join('|') + ')',
      'gi'
    );
    let lastIndex = 0;
    let match;
    const el = $container[0];

    while ((match = pattern.exec(text)) !== null) {
      if (match.index > lastIndex) {
        el.appendChild(
          document.createTextNode(text.slice(lastIndex, match.index))
        );
      }
      const mark = document.createElement('mark');
      mark.className = 'as-search-hit';
      mark.appendChild(document.createTextNode(match[0]));
      el.appendChild(mark);
      lastIndex = pattern.lastIndex;
    }

    if (lastIndex < text.length) {
      el.appendChild(document.createTextNode(text.slice(lastIndex)));
    }
  }

  $(document).ready(function () {
    const $searchInput = $('.td-search input');

    let debounceTimer = null;
    $searchInput.on('input', (event) => {
      if (debounceTimer) {
        clearTimeout(debounceTimer);
      }
      debounceTimer = setTimeout(() => {
        render($(event.target));
      }, 150);
    });

    $searchInput.on('change', (event) => {
      if (debounceTimer) {
        clearTimeout(debounceTimer);
      }
      render($(event.target));
    });

    $searchInput.closest('form').on('submit', () => {
      return false;
    });

    let idx = null;
    const resultDetails = new Map();

    $.ajax($searchInput.data('offline-search-index-json-src')).then((data) => {
      idx = lunr(function () {
        this.ref('ref');
        this.field('title', { boost: 5 });
        this.field('categories', { boost: 3 });
        this.field('tags', { boost: 3 });
        this.field('description', { boost: 2 });
        this.field('body');

        data.forEach((doc) => {
          this.add(doc);

          resultDetails.set(doc.ref, {
            title: doc.title,
            excerpt: doc.excerpt,
            body: doc.body || '',
          });
        });
      });

      $searchInput.trigger('change');
    });

    const render = ($targetSearchInput) => {
      {
        const popover = bootstrap.Popover.getInstance($targetSearchInput[0]);
        if (popover !== null) {
          popover.dispose();
        }
      }

      if (idx === null) {
        return;
      }

      const searchQuery = $targetSearchInput.val().trim();
      if (searchQuery === '') {
        return;
      }

      const tokens = queryTokens(searchQuery);
      const results = idx
        .query((q) => {
          tokens.forEach((queryString) => {
            q.term(queryString, {
              boost: 100,
            });
            q.term(queryString, {
              wildcard:
                lunr.Query.wildcard.LEADING | lunr.Query.wildcard.TRAILING,
              boost: 10,
            });
            q.term(queryString, {
              editDistance: queryString.length > 4 ? 2 : 1,
            });
          });
        })
        .slice(0, $targetSearchInput.data('offline-search-max-results'));

      const $html = $('<div>');

      $html.append(
        $('<div>')
          .addClass('as-search-results__header')
          .append($('<span>').text('Search results').css({ fontWeight: '600' }))
          .append(
            $('<span>').addClass('td-offline-search-results__close-button')
          )
      );

      const $searchResultBody = $('<div>')
        .addClass('as-search-results__list')
        .css({
          maxHeight: `calc(100vh - ${
            $targetSearchInput.offset().top - $(window).scrollTop() + 180
          }px)`,
          overflowY: 'auto',
        });
      $html.append($searchResultBody);

      if (results.length === 0) {
        $searchResultBody.append(
          $('<p>')
            .addClass('as-search-results__empty')
            .text(`No results found for query "${searchQuery}"`)
        );
      } else {
        results.forEach((r) => {
          const doc = resultDetails.get(r.ref);
          const href =
            $searchInput.data('offline-search-base-href') +
            r.ref.replace(/^\//, '');

          const $entry = $('<div>').addClass('as-search-results__item mt-3');

          $entry.append(
            $('<small>')
              .addClass('d-block as-search-results__path mono')
              .text(r.ref)
          );

          const $titleLink = $('<a>')
            .addClass('d-block as-search-results__title')
            .attr('href', href);
          appendHighlighted($titleLink, doc.title, tokens);
          $entry.append($titleLink);

          const snippet = buildSnippet(doc, searchQuery, tokens);
          const $snippet = $('<p>').addClass('as-search-results__snippet mb-0');
          appendHighlighted($snippet, snippet, tokens);
          $entry.append($snippet);

          $searchResultBody.append($entry);
        });
      }

      $targetSearchInput.one('shown.bs.popover', () => {
        $('.td-offline-search-results__close-button').on('click', () => {
          $targetSearchInput.val('');
          $targetSearchInput.trigger('change');
        });
      });

      const popover = new bootstrap.Popover($targetSearchInput, {
        content: $html[0],
        html: true,
        customClass: 'td-offline-search-results',
        placement: 'bottom',
      });
      popover.show();
    };
  });
})(jQuery);
